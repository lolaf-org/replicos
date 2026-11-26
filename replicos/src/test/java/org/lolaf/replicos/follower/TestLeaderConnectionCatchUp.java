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
package org.lolaf.replicos.follower;

import org.junit.jupiter.api.Test;
import org.lolaf.replicos.CatchUp;
import org.lolaf.replicos.wire.WireFormat;

import static org.assertj.core.api.Assertions.assertThat;

class TestLeaderConnectionCatchUp extends LeaderConnectionHarness {

    TestLeaderConnectionCatchUp() {
        super(CatchUp.byLastRecords(2));
    }

    @Test
    void aStreamNeedingASnapshotIsAnsweredWithItsCopysLastRecords() {
        main.hold(1, "one");
        main.hold(2, "two");
        main.hold(3, "three");

        read(welcome(EPOCH, WireFormat.NO_RESUME), stream(0, MAIN));

        assertThat(main.getWrites()).isEmpty();
        assertThat(sent.frames).contains(
                "STREAM_ANSWER epoch=7 key=0 SNAPSHOT_NEEDED LAST_RECORDS last=3 proof=44 bytes");
    }

    @Test
    void anEmptyCopyClaimsNothing() {
        read(welcome(EPOCH, WireFormat.NO_RESUME), stream(0, MAIN));

        assertThat(sent.frames).contains("STREAM_ANSWER epoch=7 key=0 SNAPSHOT_NEEDED LAST_RECORDS last=-1");
    }

    @Test
    void aSnapshotStartingAfterTheClaimedRecordsKeepsTheCopyThenPutsItInSync() {
        main.hold(1, "one");
        main.hold(2, "two");
        streaming();
        sent.frames.clear();

        read(snapshotBegin(0, 3, "4"), snapshotRecord(3, "three"), snapshotEnd(1, "4"));

        assertThat(main.getWrites()).containsExactly("begin from=3 state=4", "snapshot 3 three", "end state=4");
        assertThat(sent.frames).first().isEqualTo("STREAM_ANSWER epoch=7 key=0 REPLICATING LAST_RECORDS last=-1");
        assertThat(followerStreams.isInSync()).isTrue();
    }

    @Test
    void liveRecordsOfAStreamWaitingForItsSnapshotAreNotStoredSoTheNextClaimHasNoGap() {
        main.hold(1, "one");
        main.hold(2, "two");
        streaming();

        read(record(EPOCH, 100, 0, 10, "ten"), record(EPOCH, 200, 0, 11, "eleven"));
        sent.frames.clear();
        read(newConnection(0), welcome(EPOCH, WireFormat.NO_RESUME), stream(0, MAIN));

        assertThat(main.getStored()).containsExactly("1 one", "2 two");
        assertThat(sent.frames).contains(
                "STREAM_ANSWER epoch=7 key=0 SNAPSHOT_NEEDED LAST_RECORDS last=2 proof=44 bytes");
    }

    @Test
    void aSnapshotStartingPastTheClaimedRecordsIsAProtocolError() {
        main.hold(1, "one");
        streaming();
        sent.frames.clear();

        read(snapshotBegin(0, 3, "4"));

        assertThat(main.getWrites()).isEmpty();
        assertThat(sent.frames).last().asString()
                .startsWith("CLOSE epoch=7 PROTOCOL_ERROR a snapshot of stream key 0 from position 3");
    }
}
