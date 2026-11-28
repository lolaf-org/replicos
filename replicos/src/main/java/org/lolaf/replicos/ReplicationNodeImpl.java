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

import org.jspecify.annotations.Nullable;
import org.lolaf.betty.api.ss.SelectStrategy;
import org.lolaf.replicos.encryption.KeyExchange;
import org.lolaf.replicos.follower.FollowerStreams;
import org.lolaf.replicos.follower.ReplicationClient;
import org.lolaf.replicos.leader.*;
import org.lolaf.replicos.log.ReplicationLogs;
import org.lolaf.replicos.spi.*;
import org.lolaf.replicos.wire.WireFormat;
import org.lolaf.ringos.Deadline;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Owns what lives as long as the node, its logs, followers and the streams it holds as a follower, and starts a
 * server to lead or a client to follow.
 */
final class ReplicationNodeImpl implements ReplicationNode {

    private static final SecureRandom LEADERSHIPS = new SecureRandom();
    private static final ReplicaSinks NO_SINKS = new ReplicaSinks() {
        @Override
        public Binding open(String streamName, @Nullable String leaderStorageIdentity) {
            return Binding.refused(StreamAnswer.UNKNOWN_STREAM);
        }

        @Override
        public boolean isConfigured(String streamName) {
            return false;
        }
    };

    private final String nodeId;
    private final @Nullable LeaderSettings leaderSettings;
    private final @Nullable FollowerSettings followerSettings;
    private final Supplier<SelectStrategy> selectStrategy;
    private final ReplicationClock clock;
    private final KeyExchange keyExchange;
    private final CatchUp catchUp;
    private final ReplicationLogs logs;
    private final Followers followers;
    private final ReplicationStats stats;
    private final StreamRegistry streams;
    private final @Nullable StreamWriters writers;
    private final FollowerStreams followerStreams;
    private final FollowersView followersView;
    private final LeaderView leaderView;
    private volatile @Nullable ReplicationServer server;
    private volatile @Nullable ReplicationClient client;

    ReplicationNodeImpl(ReplicationNodeBuilder builder) {
        LeaderSettings leader = builder.leader();
        FollowerSettings follower = builder.follower();
        this.nodeId = builder.nodeId();
        this.leaderSettings = leader;
        this.followerSettings = follower;
        this.selectStrategy = builder.selectStrategy();
        this.clock = builder.clock();
        this.keyExchange = new KeyExchange(builder.encryption());
        this.catchUp = builder.catchUp();
        this.logs = new ReplicationLogs(leader == null ? 1 : leader.getLanes(), builder.logCapacity());
        this.followers = new Followers(clock);
        ReplicationMetrics metrics = builder.metrics();
        Duration slowFollowerHoldOff = leader == null ? Duration.ZERO : leader.getSlowFollowerHoldOff();
        this.stats = new ReplicationStats(new SlowFollowers(slowFollowerHoldOff, clock), metrics);
        int clusterId = WireFormat.clusterId(builder.cluster());
        this.streams = new StreamRegistry(clusterId, logs.count(), followers, stats);
        this.writers = leader == null ? null
                : new StreamWriters(leader, logs, followers, logFullWaits(logs.count(), metrics), stats, clock);
        this.followerStreams = new FollowerStreams(follower == null ? NO_SINKS : follower.getSinks(), clusterId,
                catchUp, metrics.receivedWriteLatency());
        this.followersView = new FollowersViewImpl(followers, stats, logs, streams);
        this.leaderView = new LeaderViewImpl(() -> client, followerStreams);
    }

    private static LatencyRecorder[] logFullWaits(int lanes, ReplicationMetrics metrics) {
        LatencyRecorder[] logFullWaits = new LatencyRecorder[lanes];
        for (int lane = 0; lane < lanes; lane++) {
            logFullWaits[lane] = metrics.logFullWait(lane);
        }
        return logFullWaits;
    }

    @Override
    public synchronized void lead(long epoch, Deadline deadline) {
        LeaderSettings leader = requireLeaderSettings();
        standDown(deadline);
        followerStreams.close(deadline);
        ReplicationServer started = new ReplicationServer(leader, streams, logs, followers, () -> epoch,
                LEADERSHIPS.nextLong(), nodeId, catchUp, selectStrategy.get(), stats, clock, keyExchange);
        started.start();
        server = started;
    }

    @Override
    public synchronized void follow(List<InetSocketAddress> leaderAddresses, LongSupplier epoch, Deadline deadline) {
        FollowerSettings follower = requireFollowerSettings();
        if (leaderAddresses.isEmpty()) {
            throw new IllegalArgumentException("No leader address to follow");
        }
        standDown(deadline);
        ReplicationClient started = new ReplicationClient(follower, leaderAddresses, followerStreams, epoch, nodeId,
                logs.getMaxRecordFrameBytes(), selectStrategy.get(), clock, keyExchange);
        started.start();
        client = started;
    }

    @Override
    public boolean handOver(Deadline deadline) {
        ReplicationServer leading = server;
        return leading != null && leading.handOver(deadline);
    }

    @Override
    public synchronized void standDown(Deadline deadline) {
        ReplicationServer leading = server;
        if (leading != null) {
            server = null;
            leading.stop(deadline);
        }
        ReplicationClient following = client;
        if (following != null) {
            client = null;
            following.stop(deadline);
        }
    }

    @Override
    public synchronized void close(Deadline deadline) {
        standDown(deadline);
        followerStreams.close(deadline);
    }

    @Override
    public boolean isLeading() {
        return server != null;
    }

    @Override
    public boolean isFollowing() {
        return client != null;
    }

    @Override
    public RegisteredStream register(StreamSettings settings) {
        StreamWriters leaderWriters = writers;
        if (leaderWriters == null) {
            throw new IllegalStateException("Node " + nodeId + " has no leader settings: it cannot replicate stream "
                    + settings.getName());
        }
        return streams.register(settings, leaderWriters);
    }

    @Override
    public @Nullable RegisteredStream stream(String streamName) {
        return streams.find(streamName);
    }

    @Override
    public List<RegisteredStream> streams() {
        return streams.list();
    }

    private LeaderSettings requireLeaderSettings() {
        LeaderSettings leader = leaderSettings;
        if (leader == null) {
            throw new IllegalStateException("Node " + nodeId + " has no leader settings: it cannot lead");
        }
        return leader;
    }

    private FollowerSettings requireFollowerSettings() {
        FollowerSettings follower = followerSettings;
        if (follower == null) {
            throw new IllegalStateException("Node " + nodeId + " has no follower settings: it cannot follow");
        }
        return follower;
    }

    @Override
    public FollowersView followers() {
        return followersView;
    }

    @Override
    public LeaderView leader() {
        return leaderView;
    }

    @Override
    public int getLanes() {
        return logs.count();
    }

    @Override
    public long getLogBytesWritten() {
        return logs.getBytesWritten();
    }
}
