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

import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.lolaf.replicos.FollowersView;
import org.lolaf.replicos.StreamWriter;
import org.lolaf.replicos.spi.StreamReplicationMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

/**
 * One thread writing at {@link #rate}, the streams in turn, each write due at its own time from the start, and in a
 * {@code SYNC} mode waiting for its ack before the next: what one application thread achieves.
 */
@Slf4j
@State(Scope.Benchmark)
public class ReplicationLoad {

    private static final long HIGHEST_LATENCY_NANOS = TimeUnit.SECONDS.toNanos(60);

    @Param({"STREAMS_1", "STREAMS_4", "STREAMS_16"})
    ReplicationParams.Streams streams;
    /**
     * Records a second, over every stream.
     */
    @Param({"RATE_10000_PER_S", "RATE_50000_PER_S", "RATE_100000_PER_S", "RATE_200000_PER_S"})
    ReplicationParams.Rate rate;
    @Param({"ASYNC", "SYNC_RECEIVED", "SYNC_APPLIED"})
    StreamReplicationMode mode;
    @Param({"PAYLOAD_200_B"})
    ReplicationParams.PayloadSize payloadSize;
    @Param({"SINK_MEMORY"})
    ReplicationParams.FollowerSink followerSink;
    /**
     * Stream {@code n} on lane {@code n} modulo the count.
     */
    @Param({"LANES_1"})
    ReplicationParams.Lanes lanes;
    @Param({"SINK_WRITER_THREADS_0"})
    ReplicationParams.SinkWriterThreads sinkWriterThreads;
    @Param({"BACKOFF"})
    ReplicationParams.Waiting waiting;
    @Param({"WRITING_5_S"})
    ReplicationParams.WritingTime writingTime;
    @Param({"CLEAR"})
    CipherParams.Encryption encryption;
    @Param({"PROVIDER_JDK"})
    CipherParams.CryptoProvider provider;

    private final Histogram writeLatency;
    private final LagMetrics leaderMetrics;
    /**
     * Each iteration's line, built without the logger's formatting, which the gc profiler would count.
     */
    private final StringBuilder line;
    private ReplicationPair pair;
    private BenchmarkStream[] written;
    private ByteBuffer payload;
    private long writes;
    private long ackFailures;
    private long iterationNanos;
    private long droppedTooSlowAtStart;
    private long snapshotsAtStart;
    private long sendWaitsAtStart;

    public ReplicationLoad() {
        this.writeLatency = new Histogram(HIGHEST_LATENCY_NANOS, 3);
        this.leaderMetrics = new LagMetrics();
        this.line = new StringBuilder(512);
    }

    @Setup(Level.Trial)
    public void setup() throws GeneralSecurityException {
        payload = ByteBuffer.allocateDirect(payloadSize.getBytes());
        pair = ReplicationPair.builder()
                .streamCount(streams.getCount())
                .mode(mode)
                .payloadBytes(payloadSize.getBytes())
                .followerSink(followerSink)
                .lanes(lanes.getCount())
                .sinkWriterThreads(sinkWriterThreads.getCount())
                .waiting(waiting)
                .encryption(encryption.settings(provider.install()))
                .leaderMetrics(leaderMetrics)
                .start();
        written = new BenchmarkStream[streams.getCount()];
        for (int key = 0; key < written.length; key++) {
            written[key] = pair.getStreams().stream(key);
        }
    }

    @Setup(Level.Iteration)
    public void startIteration() {
        writeLatency.reset();
        leaderMetrics.lagSinceLastRead();
        leaderMetrics.ackLatencySinceLastRead();
        FollowersView followers = pair.getLeader().followers();
        droppedTooSlowAtStart = followers.getDroppedTooSlow();
        snapshotsAtStart = followers.getSnapshots();
        sendWaitsAtStart = followers.getSendWaits();
        writes = 0;
        ackFailures = 0;
    }

    /**
     * Writes at {@link #rate} for {@link #writingTime}.
     *
     * @return the records written
     */
    public long run() {
        long durationNanos = TimeUnit.SECONDS.toNanos(writingTime.getSeconds());
        boolean sync = mode.isSync();
        long start = System.nanoTime();
        long count = 0;
        while (true) {
            long due = start + count * 1_000_000_000L / rate.getPerSecond();
            if (due - start >= durationNanos) {
                break;
            }
            while (System.nanoTime() < due) {
                Thread.onSpinWait();
            }
            BenchmarkStream stream = written[(int) (count % written.length)];
            long offset = stream.write(payload);
            if (sync && stream.awaitAck(offset) != StreamWriter.Ack.ACKED) {
                ackFailures++;
            }
            writeLatency.recordValue(Math.min(System.nanoTime() - due, HIGHEST_LATENCY_NANOS));
            count++;
        }
        iterationNanos = System.nanoTime() - start;
        writes = count;
        return count;
    }

    @TearDown(Level.Iteration)
    public void endIteration() {
        Histogram lag = leaderMetrics.lagSinceLastRead();
        Histogram ackLatency = leaderMetrics.ackLatencySinceLastRead();
        FollowersView followers = pair.getLeader().followers();
        line.setLength(0);
        line.append("Replication load ").append(mode).append(' ').append(streams.getCount()).append(" streams, ")
                .append(rate.getPerSecond()).append(" records/s asked, ").append(writes * 1_000_000_000L / iterationNanos)
                .append(" written, ").append(ackFailures).append(" acks failed; write latency µs p50 ")
                .append(micros(writeLatency.getValueAtPercentile(50)))
                .append(" p90 ").append(micros(writeLatency.getValueAtPercentile(90)))
                .append(" p99 ").append(micros(writeLatency.getValueAtPercentile(99)))
                .append(" p99.9 ").append(micros(writeLatency.getValueAtPercentile(99.9)))
                .append(" max ").append(micros(writeLatency.getMaxValue()))
                .append("; follower lag µs p50 ").append(micros(lag.getValueAtPercentile(50)))
                .append(" p99 ").append(micros(lag.getValueAtPercentile(99)))
                .append(" max ").append(micros(lag.getMaxValue()))
                .append("; follower round trip µs p50 ").append(micros(ackLatency.getValueAtPercentile(50)))
                .append(" p99 ").append(micros(ackLatency.getValueAtPercentile(99)))
                .append("; dropped too slow +").append(followers.getDroppedTooSlow() - droppedTooSlowAtStart)
                .append(", snapshots +").append(followers.getSnapshots() - snapshotsAtStart)
                .append(", send waits +").append(followers.getSendWaits() - sendWaitsAtStart);
        log.info(line.toString());
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pair.close();
    }

    private static long micros(long nanos) {
        return TimeUnit.NANOSECONDS.toMicros(nanos);
    }
}
