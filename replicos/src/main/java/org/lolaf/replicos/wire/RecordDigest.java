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
package org.lolaf.replicos.wire;

import org.lolaf.replicos.spi.ReplicaSink;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The proof of {@link CatchUpCheck#LAST_RECORDS}: the records digested, then the first position they start at and the
 * SHA-256 of each record's position, content type, payload length and payload, so that two copies holding the same
 * records over those positions digest the same. Not a flag, which a follower does not keep. Allocates nothing once
 * built; one thread at a time.
 */
public final class RecordDigest implements ReplicaSink.RecordConsumer {

    /**
     * Records 4, first position 8, digest 32.
     */
    public static final int PROOF_BYTES = 44;
    private static final int DIGEST_BYTES = 32;
    private static final int SCRATCH_BYTES = 1024;

    private final MessageDigest sha256;
    private final byte[] scratch;
    private final byte[] digest;
    private long firstPosition;
    private long lastPosition;
    private int recordCount;

    public RecordDigest() {
        try {
            this.sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Every JVM has SHA-256", e);
        }
        this.scratch = new byte[SCRATCH_BYTES];
        this.digest = new byte[DIGEST_BYTES];
        this.firstPosition = -1;
        this.lastPosition = -1;
    }

    public static int proofRecords(ByteBuffer proof, int index) {
        return proof.getInt(index);
    }

    public static long proofFirstPosition(ByteBuffer proof, int index) {
        return proof.getLong(index + 4);
    }

    public void reset() {
        sha256.reset();
        firstPosition = -1;
        lastPosition = -1;
        recordCount = 0;
    }

    /**
     * -1 before the first record.
     */
    public long getLastPosition() {
        return lastPosition;
    }

    public int getRecordCount() {
        return recordCount;
    }

    @Override
    public boolean onRecord(byte contentType, long position, ByteBuffer buffer, int payloadIndex, int payloadLength) {
        if (firstPosition < 0) {
            firstPosition = position;
        }
        lastPosition = position;
        recordCount++;
        longBytes(position, 0);
        scratch[8] = contentType;
        intBytes(payloadLength, 9);
        sha256.update(scratch, 0, 13);
        for (int from = 0; from < payloadLength; from += SCRATCH_BYTES) {
            int length = Math.min(SCRATCH_BYTES, payloadLength - from);
            for (int i = 0; i < length; i++) {
                scratch[i] = buffer.get(payloadIndex + from + i);
            }
            sha256.update(scratch, 0, length);
        }
        return true;
    }

    /**
     * Writes the proof of the records digested since the last {@link #reset()}, for {@code records} asked.
     *
     * @return its length, 0 for no record digested
     */
    public int writeProof(int records, ByteBuffer out, int index) {
        if (recordCount == 0) {
            return 0;
        }
        finish();
        out.putInt(index, records);
        out.putLong(index + 4, firstPosition);
        for (int i = 0; i < DIGEST_BYTES; i++) {
            out.put(index + 12 + i, digest[i]);
        }
        return PROOF_BYTES;
    }

    /**
     * Whether the records digested since the last {@link #reset()} are the proof's.
     */
    public boolean matches(ByteBuffer proof, int index) {
        if (recordCount == 0) {
            return false;
        }
        finish();
        for (int i = 0; i < DIGEST_BYTES; i++) {
            if (proof.get(index + 12 + i) != digest[i]) {
                return false;
            }
        }
        return true;
    }

    private void finish() {
        try {
            sha256.digest(digest, 0, DIGEST_BYTES);
        } catch (DigestException e) {
            throw new IllegalStateException(e);
        }
    }

    private void longBytes(long value, int at) {
        for (int i = 0; i < Long.BYTES; i++) {
            scratch[at + i] = (byte) (value >>> (56 - 8 * i));
        }
    }

    private void intBytes(int value, int at) {
        for (int i = 0; i < Integer.BYTES; i++) {
            scratch[at + i] = (byte) (value >>> (24 - 8 * i));
        }
    }
}
