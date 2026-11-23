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
import org.lolaf.replicos.spi.TestingReplicationClock;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TestAckLatencySampler {

    private final List<Long> recorded;
    private final TestingReplicationClock clock;
    private final AckLatencySampler sampler;

    TestAckLatencySampler() {
        this.recorded = new CopyOnWriteArrayList<>();
        this.clock = new TestingReplicationClock();
        this.sampler = new AckLatencySampler(recorded::add, clock);
        clock.toAdvanceableTime(Instant.EPOCH, 0);
    }

    @Test
    void anAckBeforeTheSampledRecordMeasuresNothing() {
        sampler.onSent(100);

        sampler.onAck(99);

        assertThat(recorded).isEmpty();
    }

    @Test
    void theAckCoveringTheSampledRecordMeasuresTheTimeSinceItWasSentOnce() {
        sampler.onSent(100);
        clock.advance(Duration.ofMillis(5));

        sampler.onAck(150);
        sampler.onAck(200);

        assertThat(recorded).containsExactly(TimeUnit.MILLISECONDS.toNanos(5));
    }

    @Test
    void recordsSentWhileASampleWaitsAreNotSampled() {
        sampler.onSent(100);
        clock.advance(Duration.ofSeconds(1));
        sampler.onSent(200);

        sampler.onAck(200);
        sampler.onAck(300);

        assertThat(recorded).containsExactly(TimeUnit.SECONDS.toNanos(1));
    }

    @Test
    void theNextRecordSentAfterAnAckIsSampled() {
        sampler.onSent(100);
        sampler.onAck(100);
        sampler.onSent(200);

        sampler.onAck(200);

        assertThat(recorded).hasSize(2);
    }
}
