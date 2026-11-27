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
package org.lolaf.replicos;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAckWait {

    @ParameterizedTest
    @EnumSource(value = StreamReplicationMode.class, names = {"NONE", "ASYNC"})
    void aModeThatAcknowledgesNothingIsRefused(StreamReplicationMode mode) {
        assertThatThrownBy(() -> AckWait.of(mode, Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(mode.name());
    }

    @Test
    void aNegativeQuorumIsRefused() {
        assertThatThrownBy(() -> AckWait.of(StreamReplicationMode.SYNC_APPLIED, -1, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("-1");
    }

    @Test
    void aNegativeTimeoutIsRefused() {
        assertThatThrownBy(() -> AckWait.of(StreamReplicationMode.SYNC_RECEIVED, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aTimeoutBeyondWhatNanosecondsHoldIsRefused() {
        assertThatThrownBy(() -> AckWait.of(StreamReplicationMode.SYNC_APPLIED, Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void withoutAQuorumItWaitsForEveryFollower() {
        AckWait wait = AckWait.of(StreamReplicationMode.SYNC_RECEIVED, Duration.ofMillis(5));

        assertThat(wait.getQuorum()).isEqualTo(AckWait.ALL_FOLLOWERS);
        assertThat(wait.getTimeoutNanos()).isEqualTo(5_000_000);
    }
}
