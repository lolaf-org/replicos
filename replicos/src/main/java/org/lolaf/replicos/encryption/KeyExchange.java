/*
 * Copyright © 2024-2026 Lolaf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lolaf.replicos.encryption;

import org.lolaf.replicos.EncryptionMode;
import org.lolaf.replicos.ReplicationEncryptionSettings;
import org.lolaf.replicos.wire.FrameCipher;
import org.lolaf.replicos.wire.WireFormat;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Objects;

/**
 * The encryption part of the replication handshake, the same on both nodes: the two modes decide whether a connection
 * encrypts, and when it does, both nonces and the shared secret give its keys, which the follower confirms it holds by
 * an HMAC of the HELLO and WELCOME. The confirmation covers every field of both frames and the cipher, so a frame
 * altered in transit or a cipher the peers disagree on fails it as a wrong secret does.
 */
public final class KeyExchange {

    private static final String LABEL_PREFIX = "replication ";
    private static final byte[] CONFIRMATION_LABEL = (LABEL_PREFIX + "key confirmation v1")
            .getBytes(StandardCharsets.UTF_8);
    private static final int MAC_KEY_BYTES = 32;
    private static final int IV_BYTES = 16;

    private final ReplicationEncryptionSettings settings;
    private final SecureRandom random;

    /**
     * @param settings validated
     */
    public KeyExchange(ReplicationEncryptionSettings settings) {
        this.random = new SecureRandom();
        this.settings = settings;
    }

    private static void updateWithLength(Mac mac, byte[] bytes) {
        mac.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        mac.update(bytes);
    }

    public EncryptionMode getMode() {
        return settings.getMode();
    }

    /**
     * See {@link ReplicationEncryptionSettings#describeCipher()}.
     */
    public String describeCipher() {
        return settings.describeCipher();
    }

    /**
     * Zeros when this node does not encrypt.
     */
    public byte[] newNonce() {
        byte[] nonce = new byte[WireFormat.NONCE_BYTES];
        if (settings.getMode() != EncryptionMode.DISABLED) {
            random.nextBytes(nonce);
        }
        return nonce;
    }

    public Negotiation negotiate(EncryptionMode peerMode) {
        EncryptionMode mode = settings.getMode();
        if (mode == EncryptionMode.DISABLED) {
            return peerMode == EncryptionMode.REQUIRED ? Negotiation.REFUSED : Negotiation.CLEAR;
        }
        if (peerMode != EncryptionMode.DISABLED) {
            return Negotiation.ENCRYPTED;
        }
        return mode == EncryptionMode.REQUIRED ? Negotiation.REFUSED : Negotiation.CLEAR_AGAINST_PREFERENCE;
    }

    public ConnectionEncryptionKeys deriveKeys(byte[] followerNonce, byte[] leaderNonce) {
        byte[] salt = ByteBuffer.allocate(followerNonce.length + leaderNonce.length).put(followerNonce).put(leaderNonce)
                .array();
        byte[] prk = KeyDerivation.extract(salt, Objects.requireNonNull(settings.getSecret(), "secret"));
        return new ConnectionEncryptionKeys(
                KeyDerivation.expand(prk, LABEL_PREFIX + "key confirmation", WireFormat.KEY_CONFIRMATION_BYTES),
                direction(prk, "leader-to-follower"), direction(prk, "follower-to-leader"));
    }

    /**
     * @param hello   the HELLO frame as the follower sent it
     * @param welcome the WELCOME frame as the leader sent it
     */
    public byte[] confirm(ConnectionEncryptionKeys keys, byte[] hello, byte[] welcome) {
        Mac mac = KeyDerivation.hmac(keys.getConfirmationKey());
        mac.update(CONFIRMATION_LABEL);
        updateWithLength(mac, hello);
        updateWithLength(mac, welcome);
        updateWithLength(mac, describeCipher().getBytes(StandardCharsets.UTF_8));
        return mac.doFinal();
    }

    /**
     * In constant time, so a forger learns nothing from how long a refusal takes.
     */
    public boolean confirms(ConnectionEncryptionKeys keys, byte[] hello, byte[] welcome, byte[] confirmation) {
        return MessageDigest.isEqual(confirm(keys, hello, welcome), confirmation);
    }

    /**
     * Seals what this node sends in the direction {@code keys} are for, on the one thread sending it.
     */
    public FrameCipher newSealer(ConnectionEncryptionKeys.DirectionKeys keys) {
        return newFrameCipher(keys, Cipher.ENCRYPT_MODE);
    }

    /**
     * Opens what this node receives in the direction {@code keys} are for, on the one thread reading it.
     */
    public FrameCipher newOpener(ConnectionEncryptionKeys.DirectionKeys keys) {
        return newFrameCipher(keys, Cipher.DECRYPT_MODE);
    }

    private FrameCipher newFrameCipher(ConnectionEncryptionKeys.DirectionKeys keys, int mode) {
        String provider = settings.getProvider();
        try {
            Cipher cipher = provider == null ? Cipher.getInstance(settings.getCipher())
                    : Cipher.getInstance(settings.getCipher(), provider);
            SecretKeySpec key = new SecretKeySpec(keys.getCipherKey(), settings.getKeyAlgorithm());
            if (settings.isStreamCipher()) {
                return new StreamFrameCipher(cipher, mode, key, keys.getIv(),
                        FrameMac.of(settings.getMac(), provider, keys.getMacKey()), StreamFrameCipher.SEGMENT_BYTES);
            }
            return new AeadFrameCipher(cipher, mode, key, keys.getIv());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("The settings validated " + describeCipher() + ", which now fails", e);
        }
    }

    private ConnectionEncryptionKeys.DirectionKeys direction(byte[] prk, String direction) {
        return new ConnectionEncryptionKeys.DirectionKeys(
                KeyDerivation.expand(prk, LABEL_PREFIX + direction + " cipher", settings.getKeyBits() / Byte.SIZE),
                KeyDerivation.expand(prk, LABEL_PREFIX + direction + " mac", MAC_KEY_BYTES),
                KeyDerivation.expand(prk, LABEL_PREFIX + direction + " iv", IV_BYTES));
    }
}
