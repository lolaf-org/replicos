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
package org.lolaf.replicos.leader;

import org.junit.jupiter.api.Test;
import org.lolaf.replicos.journal.Journal;
import org.lolaf.replicos.journal.JournalStream;
import org.lolaf.replicos.log.ReplicationLogs;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class TestStreamRegistry {

    private final Journal journal;

    TestStreamRegistry() {
        this.journal = new Journal(new ReplicationLogs(2, 1 << 16), Duration.ofMillis(50));
    }

    @Test
    void streamsAreKeyedAndSpreadOverTheLanesInRegistrationOrder() {
        JournalStream first = journal.add("first", StreamReplicationMode.ASYNC);
        JournalStream second = journal.add("second", StreamReplicationMode.ASYNC);
        JournalStream third = journal.add("third", StreamReplicationMode.ASYNC);
        StreamRegistry streams = journal.getStreams();

        assertThat(streams.count()).isEqualTo(3);
        assertThat(streams.get(0)).isSameAs(first.getRegistered());
        assertThat(streams.get(2)).isSameAs(third.getRegistered());
        assertThat(first.getRegistered().getLane()).isZero();
        assertThat(second.getRegistered().getLane()).isEqualTo(1);
        assertThat(third.getRegistered().getLane()).isZero();
        assertThat(streams.getClusterId()).isEqualTo(Journal.CLUSTER_ID);
    }

    @Test
    void aSetterKeepsWhatItDoesNotChange() {
        JournalStream first = journal.add("first", StreamReplicationMode.SYNC_APPLIED);
        StreamRegistration registered = journal.getStreams().get(0);

        first.getRegistered().setReplicationMode(StreamReplicationMode.ASYNC);
        first.getRegistered().setStorageIdentity("elsewhere");

        assertThat(registered.getReplication())
                .isEqualTo(new StreamRegistration.Replication(StreamReplicationMode.ASYNC, "elsewhere"));
        assertThat(registered.isReplicated()).isTrue();
        assertThat(journal.getStreams().find("first")).isSameAs(registered);
    }
}
