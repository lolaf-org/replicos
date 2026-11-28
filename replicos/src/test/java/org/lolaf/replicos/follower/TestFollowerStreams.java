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
import org.lolaf.replicos.ReplicaStream;
import org.lolaf.replicos.journal.Journal;
import org.lolaf.replicos.journal.JournalReplica;
import org.lolaf.replicos.spi.LatencyRecorder;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.spi.StreamReplicationMode;
import org.lolaf.ringos.Deadline;

import static org.assertj.core.api.Assertions.assertThat;

class TestFollowerStreams {

    private static final String MAIN = "main";
    private static final String OTHER = "other";
    private static final String LEADER_STORAGE = "leader-storage";

    private final JournalReplica replica;
    private final FollowerStreams streams;

    TestFollowerStreams() {
        this.replica = new JournalReplica();
        this.streams = new FollowerStreams(replica, Journal.CLUSTER_ID, CatchUp.fullCopy(), LatencyRecorder.NONE);
        replica.configure(MAIN);
        replica.configure(OTHER);
    }

    @Test
    void findsEachStreamByName() {
        bind(0, MAIN);
        bind(1, OTHER);

        assertThat(streams.find(MAIN)).isNotNull().extracting(ReplicaStream::getSink).isSameAs(replica.copy(MAIN));
        assertThat(streams.find(OTHER)).isNotNull().extracting(ReplicaStream::getSink).isSameAs(replica.copy(OTHER));
        assertThat(streams.find("unknown")).isNull();
    }

    @Test
    void findsAStreamANewLeaderNumbersAnotherWay() {
        bind(0, MAIN);
        bind(1, OTHER);
        ReplicaStream main = streams.find(MAIN);
        ReplicaStream other = streams.find(OTHER);

        bind(0, OTHER);
        bind(1, MAIN);

        assertThat(streams.find(MAIN)).isSameAs(main);
        assertThat(streams.find(OTHER)).isSameAs(other);
    }

    @Test
    void findsTheStreamThatReplacedOneUnderTheSameName() {
        bind(0, MAIN);
        ReplicaStream replicated = streams.find(MAIN);

        streams.bind(0, MAIN, StreamReplicationMode.NONE, null);

        assertThat(streams.find(MAIN)).isNotSameAs(replicated).satisfies(stream -> {
            assertThat(stream.isReplicated()).isFalse();
            assertThat(stream.getLeaderMode()).isEqualTo(StreamReplicationMode.NONE);
        });
    }

    @Test
    void aModeChangeKeepsTheCopy() {
        bind(0, MAIN);
        ReplicaStream replicated = streams.find(MAIN);

        streams.bind(0, MAIN, StreamReplicationMode.SYNC_APPLIED, LEADER_STORAGE);

        assertThat(streams.find(MAIN)).isSameAs(replicated);
        assertThat(replica.copy(MAIN).isStopped()).isFalse();
    }

    @Test
    void aStorageIdentityChangeOpensTheStreamAgainForTheSinksToCheck() {
        bind(0, MAIN);
        ReplicaStream replicated = streams.find(MAIN);

        assertThat(streams.bind(0, MAIN, StreamReplicationMode.ASYNC, "follower-storage"))
                .isEqualTo(StreamAnswer.SHARED_STORAGE);

        assertThat(streams.find(MAIN)).isNotSameAs(replicated).extracting(ReplicaStream::isReplicated).isEqualTo(false);
        assertThat(replica.copy(MAIN).isStopped()).isTrue();
        assertThat(streams.bind(0, MAIN, StreamReplicationMode.ASYNC, "moved-storage"))
                .as("caught up again").isEqualTo(StreamAnswer.SNAPSHOT_NEEDED);
        assertThat(replica.copy(MAIN).isStopped()).isFalse();
    }

    @Test
    void aStorageIdentityChangeOpensAgainAStreamANewLeaderNumbersAnotherWay() {
        bind(0, MAIN);
        bind(1, OTHER);

        assertThat(streams.bind(0, OTHER, StreamReplicationMode.ASYNC, "follower-storage"))
                .isEqualTo(StreamAnswer.SHARED_STORAGE);

        assertThat(replica.copy(OTHER).isStopped()).isTrue();
    }

    @Test
    void findsNothingOnceClosed() {
        bind(0, MAIN);

        streams.close(Deadline.unlimited());

        assertThat(streams.find(MAIN)).isNull();
    }

    private void bind(int key, String streamName) {
        streams.bind(key, streamName, StreamReplicationMode.ASYNC, LEADER_STORAGE);
    }
}
