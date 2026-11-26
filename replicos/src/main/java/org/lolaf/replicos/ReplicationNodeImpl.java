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
import org.lolaf.ringos.Deadline;
import org.lolaf.replicos.encryption.KeyExchange;
import org.lolaf.replicos.follower.FollowerStreams;
import org.lolaf.replicos.follower.ReplicationClient;
import org.lolaf.replicos.leader.Followers;
import org.lolaf.replicos.leader.ReplicationServer;
import org.lolaf.replicos.leader.ReplicationStats;
import org.lolaf.replicos.leader.SlowFollowers;
import org.lolaf.replicos.leader.StreamLogWriter;
import org.lolaf.replicos.log.ReplicationLogs;
import org.lolaf.replicos.spi.LeaderStream;
import org.lolaf.replicos.spi.LeaderStreams;
import org.lolaf.replicos.spi.ReplicaSinks;
import org.lolaf.replicos.spi.ReplicationClock;
import org.lolaf.replicos.spi.LatencyRecorder;
import org.lolaf.replicos.spi.ReplicationMetrics;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.spi.StreamReplicationMode;

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
    private static final LeaderStreams NO_STREAMS = new LeaderStreams() {
        @Override
        public int getApplicationId() {
            return 0;
        }

        @Override
        public int count() {
            return 0;
        }

        @Override
        public LeaderStream get(int key) {
            throw new IndexOutOfBoundsException("No stream " + key + ": this node has no leader settings");
        }
    };
    private static final ReplicaSinks NO_SINKS = new ReplicaSinks() {
        @Override
        public int getApplicationId() {
            return 0;
        }

        @Override
        public Binding open(String streamName, StreamReplicationMode leaderMode, @Nullable String leaderStorageIdentity) {
            return Binding.refused(StreamAnswer.UNKNOWN_STREAM);
        }

        @Override
        public @Nullable StreamReplicationMode modeHere(String streamName) {
            return null;
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
    private final LatencyRecorder[] logFullWaits;
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
        this.logFullWaits = new LatencyRecorder[logs.count()];
        for (int lane = 0; lane < logFullWaits.length; lane++) {
            logFullWaits[lane] = metrics.logFullWait(lane);
        }
        this.followerStreams = new FollowerStreams(follower == null ? NO_SINKS : follower.getSinks(),
                catchUp, metrics.receivedWriteLatency());
        this.followersView = new FollowersViewImpl(followers, stats, logs, leader == null ? NO_STREAMS : leader.getStreams());
        this.leaderView = new LeaderViewImpl(() -> client, followerStreams);
    }

    @Override
    public synchronized void lead(long epoch, Deadline deadline) {
        LeaderSettings leader = requireLeaderSettings();
        standDown(deadline);
        followerStreams.close(deadline);
        ReplicationServer started = new ReplicationServer(leader, logs, followers, () -> epoch, LEADERSHIPS.nextLong(),
                nodeId, catchUp, selectStrategy.get(), stats, clock, keyExchange);
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
    public StreamWriter writer(int streamKey) {
        LeaderSettings leader = requireLeaderSettings();
        return new StreamLogWriter(streamKey, logs.forStream(streamKey), followers, leader.getLogFullTimeout(),
                logFullWaits[logs.laneOf(streamKey)], leader.getAckIdleStrategy().get(), stats, clock);
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
    public void reannounce(int streamKey) {
        followers.reannounce(streamKey);
    }

    @Override
    public void resync(int streamKey) {
        followers.resync(streamKey);
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

    @Override
    public String describeCipher() {
        return keyExchange.describeCipher();
    }

    @Override
    public boolean cipherAllocatesOnEveryFrame() {
        return keyExchange.allocatesOnEveryFrame();
    }
}
