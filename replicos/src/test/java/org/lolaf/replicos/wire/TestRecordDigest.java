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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class TestRecordDigest {

    private static final byte TEXT = 1;

    private static ByteBuffer proofOf(RecordDigest digest) {
        ByteBuffer proof = ByteBuffer.allocate(RecordDigest.PROOF_BYTES);
        assertThat(digest.writeProof(2, proof, 0)).isEqualTo(RecordDigest.PROOF_BYTES);
        return proof;
    }

    private static void digest(RecordDigest digest, byte contentType, long position, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
        digest.onRecord(contentType, position, ByteBuffer.wrap(bytes), 0, bytes.length);
    }

    @Test
    void theSameRecordsMatchTheirProof() {
        RecordDigest follower = new RecordDigest();
        digest(follower, TEXT, 4, "four");
        digest(follower, TEXT, 5, "five");
        ByteBuffer proof = proofOf(follower);
        RecordDigest leader = new RecordDigest();
        digest(leader, TEXT, 4, "four");
        digest(leader, TEXT, 5, "five");

        assertThat(leader.matches(proof, 0)).isTrue();
        assertThat(RecordDigest.proofRecords(proof, 0)).isEqualTo(2);
        assertThat(RecordDigest.proofFirstPosition(proof, 0)).isEqualTo(4);
    }

    @Test
    void anotherPayloadPositionOrContentTypeDoesNotMatch() {
        RecordDigest follower = new RecordDigest();
        digest(follower, TEXT, 4, "four");
        ByteBuffer proof = proofOf(follower);
        RecordDigest leader = new RecordDigest();

        digest(leader, TEXT, 4, "fours");
        assertThat(leader.matches(proof, 0)).as("payload").isFalse();
        leader.reset();
        digest(leader, TEXT, 5, "four");
        assertThat(leader.matches(proof, 0)).as("position").isFalse();
        leader.reset();
        digest(leader, (byte) 2, 4, "four");
        assertThat(leader.matches(proof, 0)).as("content type").isFalse();
        leader.reset();
        digest(leader, TEXT, 4, "four");
        digest(leader, TEXT, 5, "one more");
        assertThat(leader.matches(proof, 0)).as("a record more").isFalse();
    }

    @Test
    void nothingDigestedProvesNothingAndMatchesNothing() {
        RecordDigest digest = new RecordDigest();

        assertThat(digest.writeProof(2, ByteBuffer.allocate(RecordDigest.PROOF_BYTES), 0)).isZero();
        assertThat(digest.getLastPosition()).isEqualTo(-1);
        assertThat(digest.matches(ByteBuffer.allocate(RecordDigest.PROOF_BYTES), 0)).isFalse();
    }

    @Test
    void aPayloadLongerThanTheScratchDigestsAsItsBytesWhereverItLies() {
        byte[] bytes = new byte[2_500];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        RecordDigest heap = new RecordDigest();
        heap.onRecord(TEXT, 1, ByteBuffer.wrap(bytes), 0, bytes.length);
        ByteBuffer proof = proofOf(heap);
        ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length + 10);
        direct.position(10);
        direct.put(bytes);
        direct.position(3);
        RecordDigest offHeap = new RecordDigest();

        offHeap.onRecord(TEXT, 1, direct, 10, bytes.length);

        assertThat(offHeap.matches(proof, 0)).isTrue();
        assertThat(direct.position()).as("left untouched").isEqualTo(3);
        assertThat(direct.limit()).isEqualTo(direct.capacity());
    }
}
