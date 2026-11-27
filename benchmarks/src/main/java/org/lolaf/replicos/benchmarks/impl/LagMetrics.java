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
package org.lolaf.replicos.benchmarks.impl;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.lolaf.replicos.spi.LatencyRecorder;
import org.lolaf.replicos.spi.ReplicationMetrics;

import java.util.concurrent.TimeUnit;

/**
 * The leader's view of its follower, over every lane: how far behind it is, and its round trip. Recorded by the IO
 * threads, read by the benchmark between iterations.
 */
public final class LagMetrics implements ReplicationMetrics {

    private static final long HIGHEST_NANOS = TimeUnit.SECONDS.toNanos(60);

    private final Recorder lag;
    private final Recorder ackLatency;
    /**
     * Handed back to its recorder at each read, which otherwise allocates a histogram per read.
     */
    private Histogram lagInterval;
    private Histogram ackLatencyInterval;

    public LagMetrics() {
        this.lag = new Recorder(HIGHEST_NANOS, 3);
        this.ackLatency = new Recorder(HIGHEST_NANOS, 3);
        this.lagInterval = lag.getIntervalHistogram();
        this.ackLatencyInterval = ackLatency.getIntervalHistogram();
    }

    @Override
    public LatencyRecorder followerLag(String followerId, int lane) {
        return durationInNanos -> lag.recordValue(Math.min(durationInNanos, HIGHEST_NANOS));
    }

    @Override
    public LatencyRecorder followerAckLatency(String followerId, int lane) {
        return durationInNanos -> ackLatency.recordValue(Math.min(durationInNanos, HIGHEST_NANOS));
    }

    /**
     * Since the last call, which starts a new interval and reuses the histogram it returned.
     */
    public Histogram lagSinceLastRead() {
        lagInterval = lag.getIntervalHistogram(lagInterval);
        return lagInterval;
    }

    /**
     * Since the last call, which starts a new interval and reuses the histogram it returned.
     */
    public Histogram ackLatencySinceLastRead() {
        ackLatencyInterval = ackLatency.getIntervalHistogram(ackLatencyInterval);
        return ackLatencyInterval;
    }
}
