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

import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorkersGroup;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.betty.api.ss.SelectStrategy;
import org.lolaf.replicos.FollowerSettings;
import org.lolaf.replicos.LeaderStatus;
import org.lolaf.replicos.encryption.KeyExchange;
import org.lolaf.replicos.spi.ReplicationClock;
import org.lolaf.replicos.wire.FrameDecoder;
import org.lolaf.replicos.wire.WireFormat;
import org.lolaf.ringos.Deadline;

import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * The follower's connections to the leader, one per replication lane the leader announces, each made again whenever
 * it drops through a new {@link LeaderConnection} on the same {@link FollowerStreams}. Lane 0 connects first; its
 * WELCOME, or any lane's, says how many the leader has, and the others are opened or closed to match. Each lane has its
 * own IO thread, so the lanes are applied in parallel.
 */
@Slf4j
public final class ReplicationClient implements LeaderLanes {

    private static final Duration LANE_STOP = Duration.ofSeconds(1);
    private static final long DETACH_POLL_NANOS = 50_000;

    private final List<InetSocketAddress> leaderAddresses;
    private final FollowerStreams followerStreams;
    private final LongSupplier nodeEpoch;
    private final String nodeId;
    private final int maxReceivedFrameBytes;
    private final Duration reconnectInterval;
    private final long checkIntervalNanos;
    private final SelectStrategy selectStrategy;
    private final @Nullable SinkWriterThreads sinkWriterThreads;
    private final ReplicationClock clock;
    private final KeyExchange keyExchange;
    private volatile LaneClient[] laneClients;
    private boolean stopped;

    /**
     * @param leaderAddresses     the leader's, tried in turn by each lane until one answers: a leader reachable over
     *                            several networks
     * @param maxRecordFrameBytes the largest frame the leader sends before its tag, see
     *                            {@link org.lolaf.replicos.log.ReplicationLog#getMaxRecordFrameBytes()}
     */
    public ReplicationClient(FollowerSettings settings, List<InetSocketAddress> leaderAddresses,
                             FollowerStreams followerStreams, LongSupplier nodeEpoch, String nodeId,
                             int maxRecordFrameBytes, SelectStrategy selectStrategy, ReplicationClock clock,
                             KeyExchange keyExchange) {
        this.clock = clock;
        this.keyExchange = keyExchange;
        this.laneClients = new LaneClient[0];
        this.sinkWriterThreads = settings.getSinkWriterThreads() == 0 ? null
                : new SinkWriterThreads(settings.getSinkWriterThreads(), settings.getSinkWriterIdleStrategy(), nodeId);
        this.selectStrategy = selectStrategy;
        this.leaderAddresses = List.copyOf(leaderAddresses);
        this.followerStreams = followerStreams;
        this.nodeEpoch = nodeEpoch;
        this.nodeId = nodeId;
        this.maxReceivedFrameBytes = maxRecordFrameBytes + WireFormat.TAG_BYTES;
        this.reconnectInterval = settings.getReconnectInterval();
        this.checkIntervalNanos = settings.getCheckInterval().toNanos();
    }

    public synchronized void start() {
        laneClients = new LaneClient[]{new LaneClient(0)};
        log.info("Replication follower {} connecting to its leader at {}", nodeId, leaderAddresses);
    }

    /**
     * Stops reading, then waits, within the deadline, for each lane to write what it read: a record acknowledged as
     * received is written before a promotion hands the streams over.
     */
    public void stop(Deadline deadline) {
        LaneClient[] stopping;
        // not held while stopping: a sink writer thread starting a stream takes it in lanesAnnounced
        synchronized (this) {
            stopped = true;
            stopping = laneClients;
        }
        for (LaneClient laneClient : stopping) {
            laneClient.stop(deadline, true);
        }
        if (sinkWriterThreads != null) {
            sinkWriterThreads.stop();
        }
    }

    @Override
    public @Nullable String refuseLeader(int lane, FollowedLeader leader) {
        if (lane == 0) {
            return detachLanesFrom(leader);
        }
        LeaderConnection first = getConnection(0);
        FollowedLeader followed = first == null ? null : first.getLeader();
        if (leader.equals(followed)) {
            return null;
        }
        return followed == null ? "lane 0 follows no leader yet"
                : "leader " + leader + " on lane " + lane + ", lane 0 follows leader " + followed;
    }

    /**
     * Lane 0 binding a stream the leader numbers otherwise moves it from the lane it was on: that lane's thread must
     * no longer apply another leader's frames to it.
     */
    private @Nullable String detachLanesFrom(FollowedLeader leader) {
        LaneClient[] current = laneClients;
        Deadline deadline = Deadline.of(LANE_STOP);
        for (int lane = 1; lane < current.length; lane++) {
            String refusal = current[lane].detachFrom(leader, deadline);
            if (refusal != null) {
                return refusal;
            }
        }
        return null;
    }

    /**
     * Called on the IO thread of a lane below the count, so it never stops its caller.
     */
    @Override
    public synchronized void lanesAnnounced(int lanes) {
        followerStreams.setLanes(lanes);
        LaneClient[] current = laneClients;
        if (stopped || lanes == current.length) {
            return;
        }
        LaneClient[] resized = Arrays.copyOf(current, lanes);
        for (int lane = current.length; lane < lanes; lane++) {
            resized[lane] = new LaneClient(lane);
        }
        for (int lane = lanes; lane < current.length; lane++) {
            current[lane].stop(Deadline.of(LANE_STOP), false);
        }
        laneClients = resized;
        log.info("Replication follower {} replicates over {} lanes from its leader at {}", nodeId, lanes, leaderAddresses);
    }

    /**
     * The lane's connection to the leader now, null between connections or for a lane not opened.
     */
    @Nullable LeaderConnection getConnection(int lane) {
        LaneClient[] current = laneClients;
        return lane < current.length ? current[lane].current() : null;
    }

    public LeaderStatus getStatus() {
        LeaderConnection first = getConnection(0);
        return LeaderStatus.builder()
                .leaderId(first == null ? null : first.getLeaderId())
                .address(String.valueOf(first == null ? leaderAddresses : first.getAddress()))
                .connected(isConnected())
                .inSync(isInSync())
                .cipher(isEncrypted() ? keyExchange.describeCipher() : null)
                .build();
    }

    /**
     * Every lane the leader announced is streaming.
     */
    public boolean isConnected() {
        LaneClient[] current = laneClients;
        for (LaneClient laneClient : current) {
            LeaderConnection connection = laneClient.current();
            if (connection == null || connection.getState() != LeaderConnection.State.STREAMING) {
                return false;
            }
        }
        return current.length > 0;
    }

    public boolean isInSync() {
        return isConnected() && followerStreams.isInSync();
    }

    /**
     * Every lane the leader announced is streaming encrypted.
     */
    public boolean isEncrypted() {
        LaneClient[] current = laneClients;
        for (LaneClient laneClient : current) {
            LeaderConnection connection = laneClient.current();
            if (connection == null || !connection.isEncrypted()) {
                return false;
            }
        }
        return current.length > 0;
    }

    /**
     * One lane's client, connecting to the leader again whenever its connection drops.
     */
    private final class LaneClient implements IOEventsListener {

        private final int lane;
        private final FrameDecoder decoder;
        private final IOWorkersGroup ioWorkers;
        private final Client client;
        private final @Nullable LaneWriteQueue writeQueue;
        /**
         * The last connection made, kept until its thread is done with it: a sink writer thread may still apply what
         * it read once disconnected.
         */
        private volatile @Nullable LeaderConnection connection;
        private boolean connectedBefore;

        LaneClient(int lane) {
            this.decoder = new FrameDecoder(maxReceivedFrameBytes);
            this.lane = lane;
            this.writeQueue = sinkWriterThreads == null ? null
                    : new LaneWriteQueue(lane, maxReceivedFrameBytes, followerStreams, sinkWriterThreads.forLane(lane));
            String id = "replication-follower-" + lane;
            ioWorkers = IOWorkersGroupSettings.builder().id(id)
                    .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder().selectStrategy(selectStrategy).build())
                    .build().newInstance();
            client = ClientBuilder.builder()
                    .id(id)
                    .connectAddresses(leaderAddresses)
                    .connectionRetry(reconnectInterval)
                    .ioWorkersGroup(ioWorkers)
                    .ioSettings(IOSettings.builder()
                            .readBufferSize(WireFormat.READ_BUFFER_BYTES)
                            .maxReadBufferSize(WireFormat.bufferBytes(maxReceivedFrameBytes))
                            .socketOptions(Map.of(StandardSocketOptions.TCP_NODELAY, true))
                            .build())
                    .ioEventsListener(this)
                    .build().newInstance();
            client.start();
        }

        /**
         * @param drain whether to wait, within the deadline, for the lane's sink writer thread to write what was read;
         *              not when the lane is dropped from a sink writer thread, which may be the one to drain it
         */
        void stop(Deadline deadline, boolean drain) {
            client.stop(deadline);
            ioWorkers.stop(deadline);
            if (writeQueue != null) {
                writeQueue.stop(deadline, drain);
            }
        }

        @Nullable LeaderConnection current() {
            LeaderConnection last = connection;
            return last == null || last.isDetached() ? null : last;
        }

        /**
         * Ends the lane's connection unless it follows that leader, then waits for its thread to be done with it,
         * unless that is the caller's own: it cannot be applying a frame then, and drops those it has queued.
         *
         * @return why the lane still applies another leader's frames past the deadline, null once it does not
         */
        @Nullable String detachFrom(FollowedLeader leader, Deadline deadline) {
            LeaderConnection last = current();
            if (last == null || leader.equals(last.getLeader())) {
                return null;
            }
            last.fence();
            if (writeQueue != null && writeQueue.isWrittenByCurrentThread()) {
                return null;
            }
            while (!last.isDetached() && !deadline.isExpired()) {
                LockSupport.parkNanos(DETACH_POLL_NANOS);
            }
            return last.isDetached() ? null : "lane " + lane + " still applies what leader " + last.getLeader() + " sent";
        }

        @Override
        public void onConnected(IOSession session) {
            if (connectedBefore) {
                followerStreams.getStats().reconnected();
            }
            connectedBefore = true;
            boolean writtenBySinkWriter = writeQueue != null;
            AnswerSender answers = writtenBySinkWriter ?
                    new ThroughNetworkThreadAnswerSender(session) : new OnNetworkThreadAnswerSender(session);
            LeaderConnection connected = new LeaderConnection(session, followerStreams, nodeEpoch, nodeId, lane,
                    ReplicationClient.this, checkIntervalNanos, answers, writtenBySinkWriter, clock, keyExchange);
            session.setAttachment(connected);
            connection = connected;
            if (writtenBySinkWriter) {
                writeQueue.connected(connected);
            } else {
                connected.hello();
            }
        }

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            LeaderConnection reading = session.getAttachment();
            if (writeQueue == null) {
                reading.read(message, decoder);
            } else {
                writeQueue.queueRead(reading, session, message);
            }
        }

        @Override
        public void onDisconnected(IOSession session) {
            if (session == null) {
                return;
            }
            LeaderConnection disconnected = session.getAttachment();
            if (disconnected != null) {
                if (writeQueue == null) {
                    disconnected.disconnected();
                } else {
                    writeQueue.disconnected();
                }
            }
        }
    }
}
