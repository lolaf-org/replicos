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
package org.lolaf.replicos.benchmarks;

import org.lolaf.replicos.EncryptionMode;
import org.lolaf.replicos.benchmarks.impl.CipherParams;
import org.lolaf.replicos.benchmarks.impl.ReplicationParams;
import org.lolaf.replicos.encryption.ConnectionEncryptionKeys;
import org.lolaf.replicos.encryption.KeyExchange;
import org.lolaf.replicos.wire.FrameCipher;
import org.lolaf.replicos.wire.FrameEncoder;
import org.lolaf.replicos.wire.WireFormat;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

/**
 * One record frame sealed as the leader's sender does, and sealed then opened as the follower does, on a direct
 * buffer like the replication's: per cipher, JCE provider and payload size. {@code CLEAR} is the frame's encoding alone,
 * which every other figure includes. Not every provider has every cipher: run each provider with its own {@code -p}.
 */
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsPrepend = {"-Xmx1g", "-Xms1g"})
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class FrameCipherBenchmark {

    /**
     * Never read by the replication: any of the application's bytes.
     */
    private static final byte CONTENT_TYPE = 0;
    private final ByteBuffer frames;
    @Param({"CLEAR", "AES_GCM_128", "AES_GCM_256", "CHACHA20_POLY1305", "AES_CTR_HMAC_SHA256", "CHACHA20_HMAC_SHA256"})
    CipherParams.Encryption encryption;
    @Param({"PROVIDER_JDK"})
    CipherParams.CryptoProvider provider;
    @Param({"PAYLOAD_200_B", "PAYLOAD_1_KIB", "PAYLOAD_16_KIB"})
    ReplicationParams.PayloadSize payloadSize;
    private ByteBuffer payload;
    private FrameCipher sealer;
    private FrameCipher opener;
    private long sequenceNumber;

    public FrameCipherBenchmark() {
        this.frames = ByteBuffer.allocateDirect(64 * 1024);
        this.sealer = FrameCipher.CLEAR;
        this.opener = FrameCipher.CLEAR;
        this.sequenceNumber = 0;
    }

    @Setup(Level.Trial)
    public void setup() throws GeneralSecurityException {
        payload = ByteBuffer.allocateDirect(payloadSize.getBytes());
        KeyExchange keyExchange = new KeyExchange(encryption.settings(provider.install()));
        if (keyExchange.getMode() != EncryptionMode.DISABLED) {
            ConnectionEncryptionKeys keys = keyExchange.deriveKeys(keyExchange.newNonce(), keyExchange.newNonce());
            sealer = keyExchange.newSealer(keys.getLeaderToFollower());
            opener = keyExchange.newOpener(keys.getLeaderToFollower());
        }
    }

    @Benchmark
    public int seal() {
        return sealer.seal(frames, 0, record());
    }

    @Benchmark
    public int sealAndOpen() {
        return opener.open(frames, 0, sealer.seal(frames, 0, record()));
    }

    private int record() {
        long seq = ++sequenceNumber;
        int body = FrameEncoder.recordBody(frames, WireFormat.RECORD_BODY_OFFSET, 0,
                CONTENT_TYPE, (byte) 0, seq, payload, 0, payload.capacity());
        return FrameEncoder.recordHeader(frames, 0, 1, seq, body);
    }
}
