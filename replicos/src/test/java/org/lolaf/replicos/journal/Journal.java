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
package org.lolaf.replicos.journal;

import lombok.Getter;
import org.jspecify.annotations.Nullable;
import org.lolaf.replicos.LeaderSettings;
import org.lolaf.replicos.RegisteredStream;
import org.lolaf.replicos.ReplicationNode;
import org.lolaf.replicos.StreamSettings;
import org.lolaf.replicos.leader.*;
import org.lolaf.replicos.log.ReplicationLogs;
import org.lolaf.replicos.spi.LatencyRecorder;
import org.lolaf.replicos.spi.ReplicationClock;
import org.lolaf.replicos.spi.ReplicationMetrics;
import org.lolaf.replicos.spi.StreamReplicationMode;
import org.lolaf.replicos.wire.WireFormat;
import org.lolaf.ringos.idling.BackoffIdleStrategy;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * The application the library's tests replicate: append-only journals of text entries, each journal a stream, on the
 * leader. {@link JournalReplica} is its follower side. It writes straight to the logs, for the tests of the leader's
 * parts, or through the node {@link #writeThrough written through}.
 */
public final class Journal {

    public static final String CLUSTER = "journal";
    public static final int CLUSTER_ID = WireFormat.clusterId(CLUSTER);

    /**
     * Null for a journal written through a node.
     */
    @Getter
    private final @Nullable ReplicationLogs logs;
    /**
     * Null for a journal written through a node.
     */
    @Getter
    private final @Nullable Followers followers;
    /**
     * What the leader's parts read, null for a journal written through a node.
     */
    @Getter
    private final @Nullable StreamRegistry streams;
    /**
     * What its writers counted; null for a journal written through a node.
     */
    @Getter
    private final @Nullable ReplicationStats writerStats;
    /**
     * The log full waits its writers timed.
     */
    @Getter
    private final List<Long> logFullWaitNanos;
    private volatile Function<StreamSettings, RegisteredStream> registrar;

    /**
     * @param logFullTimeout how long an append waits for a follower a whole log behind before the log drops it
     */
    public Journal(ReplicationLogs logs, Duration logFullTimeout) {
        Followers direct = new Followers(ReplicationClock.SYSTEM);
        ReplicationStats stats = new ReplicationStats(new SlowFollowers(Duration.ZERO, ReplicationClock.SYSTEM),
                ReplicationMetrics.NONE);
        List<Long> waits = new CopyOnWriteArrayList<>();
        LatencyRecorder[] logFullWaits = new LatencyRecorder[logs.count()];
        Arrays.fill(logFullWaits, (LatencyRecorder) waits::add);
        StreamRegistry registry = new StreamRegistry(CLUSTER_ID, logs.count(), direct, stats);
        StreamWriters writers = new StreamWriters(settings().logFullTimeout(logFullTimeout)
                .ackIdleStrategy(BackoffIdleStrategy::new).build(), logs, direct, logFullWaits, stats,
                ReplicationClock.SYSTEM);
        this.logs = logs;
        this.followers = direct;
        this.streams = registry;
        this.writerStats = stats;
        this.logFullWaitNanos = waits;
        this.registrar = settings -> registry.register(settings, writers);
    }

    /**
     * Written through the node {@link #writeThrough} names, before any stream is added.
     */
    public Journal() {
        this.logs = null;
        this.followers = null;
        this.streams = null;
        this.writerStats = null;
        this.logFullWaitNanos = List.of();
        this.registrar = settings -> {
            throw new IllegalStateException("No node to write through yet");
        };
    }

    /**
     * One lane, a follower a whole log behind dropped after 50 ms.
     */
    public static Journal ofCapacity(int logCapacity) {
        return new Journal(new ReplicationLogs(1, logCapacity), Duration.ofMillis(50));
    }

    /**
     * Settings leading this journal on any port, a follower never dropped as stalled and a snapshot sent in one chunk.
     */
    public LeaderSettings.LeaderSettingsBuilder settings() {
        return LeaderSettings.builder()
                .bindAddress(new InetSocketAddress(0))
                .followerStallTimeout(Duration.ZERO)
                .snapshotChunkBytes(Integer.MAX_VALUE);
    }

    /**
     * The node replicating this journal, built with it.
     */
    public void writeThrough(ReplicationNode node) {
        registrar = node::register;
    }

    public JournalStream add(String name, StreamReplicationMode mode, @Nullable String storageIdentity) {
        return new JournalStream(name, mode, storageIdentity, registrar);
    }

    /**
     * Stored at {@code "leader-storage"}.
     */
    public JournalStream add(String name, StreamReplicationMode mode) {
        return add(name, mode, "leader-storage");
    }
}
