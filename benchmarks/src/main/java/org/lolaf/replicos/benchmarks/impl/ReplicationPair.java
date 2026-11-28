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

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.ss.IdleStrategySelectStrategy;
import org.lolaf.replicos.*;
import org.lolaf.replicos.spi.ReplicationMetrics;
import org.lolaf.replicos.spi.StreamReplicationMode;
import org.lolaf.ringos.Deadline;
import org.lolaf.ringos.idling.BusySpinIdleStrategy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * A leader and its follower in this JVM, over loopback, replicating {@link BenchmarkStreams} into
 * {@link BenchmarkSinks}. A network between two hosts would add its round trip to every {@code SYNC} figure.
 */
@Slf4j
public final class ReplicationPair implements AutoCloseable {

    private static final Duration START = Duration.ofSeconds(30);
    private static final Duration STOP = Duration.ofSeconds(5);

    @Getter
    private final BenchmarkStreams streams;
    @Getter
    private final ReplicationNode leader;
    private final ReplicationNode follower;
    private final BenchmarkSinks sinks;

    private ReplicationPair(BenchmarkStreams streams, BenchmarkSinks sinks, ReplicationNode leader,
                            ReplicationNode follower) {
        this.streams = streams;
        this.sinks = sinks;
        this.leader = leader;
        this.follower = follower;
    }

    /**
     * Returns once the follower holds every stream in sync.
     *
     * @param leaderMetrics null for none
     */
    @Builder(builderMethodName = "builder", buildMethodName = "start")
    private static ReplicationPair start(int streamCount, StreamReplicationMode mode, int payloadBytes,
                                         ReplicationParams.FollowerSink followerSink, int lanes, int sinkWriterThreads,
                                         ReplicationParams.Waiting waiting, ReplicationEncryptionSettings encryption,
                                         ReplicationMetrics leaderMetrics) {
        BenchmarkStreams streams = new BenchmarkStreams(streamCount, mode, payloadBytes);
        BenchmarkSinks sinks;
        try {
            sinks = new BenchmarkSinks(followerSink, payloadBytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        InetSocketAddress leaderAddress = new InetSocketAddress("localhost", freePort());
        boolean spin = waiting == ReplicationParams.Waiting.SPIN;
        LeaderSettings.LeaderSettingsBuilder leaderSettings = LeaderSettings.builder()
                .bindAddress(leaderAddress)
                .lanes(lanes);
        FollowerSettings.FollowerSettingsBuilder followerSettings = FollowerSettings.builder()
                .sinks(sinks)
                .sinkWriterThreads(sinkWriterThreads);
        if (spin) {
            leaderSettings.senderIdleStrategy(BusySpinIdleStrategy::getInstance)
                    .ackIdleStrategy(BusySpinIdleStrategy::getInstance);
            followerSettings.sinkWriterIdleStrategy(BusySpinIdleStrategy::getInstance);
        }
        ReplicationNode leader = node("leader", spin, encryption)
                .leader(leaderSettings.build())
                .metrics(leaderMetrics == null ? ReplicationMetrics.NONE : leaderMetrics)
                .build();
        ReplicationNode follower = node("follower", spin, encryption)
                .follower(followerSettings.build())
                .build();
        streams.writeThrough(leader);
        leader.lead(1, Deadline.of(START));
        follower.follow(leaderAddress, () -> 1, Deadline.of(START));
        ReplicationPair pair = new ReplicationPair(streams, sinks, leader, follower);
        pair.await("the follower in sync with every stream", pair::isEveryStreamInSync);
        log.info("Replication pair ready: {} streams {} over {} lanes, {} B payloads, follower {} in {} with {} sink writer "
                        + "threads, {} waiting, {}", streamCount, mode, lanes, payloadBytes, followerSink, pair.describeStorage(),
                sinkWriterThreads, waiting,
                encryption.getMode() == EncryptionMode.DISABLED ? "clear" : "encrypted with " + leader.describeCipher());
        return pair;
    }

    private static ReplicationNodeBuilder node(String nodeId, boolean spin, ReplicationEncryptionSettings encryption) {
        ReplicationNodeBuilder builder = ReplicationNode.builder()
                .nodeId(nodeId)
                .cluster(BenchmarkStreams.CLUSTER)
                .encryption(encryption);
        if (spin) {
            builder.selectStrategy(() -> new IdleStrategySelectStrategy(BusySpinIdleStrategy::getInstance));
        }
        return builder;
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String describeStorage() {
        try {
            return sinks.describeStorage();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isEveryStreamInSync() {
        return follower.leader().isInSync() && leader.streams().stream().allMatch(RegisteredStream::isInSync);
    }

    private void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + START.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                close();
                throw new IllegalStateException("Timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    @Override
    public void close() {
        follower.close(Deadline.of(STOP));
        leader.close(Deadline.of(STOP));
        try {
            sinks.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
