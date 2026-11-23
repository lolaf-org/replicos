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

import org.jspecify.annotations.Nullable;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.replicos.StreamWriter.Ack;
import org.lolaf.replicos.spi.ReplicationClock;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * The followers connected to the leader, and what a {@code SYNC} send waits on: their acks.
 */
public final class Followers {

    private static final FollowerConnection[] NONE = new FollowerConnection[0];
    private static final IdleStrategy[] NO_WAITERS = new IdleStrategy[0];

    private volatile FollowerConnection[] connections;
    private volatile List<FollowerConnection> connectionList;
    private volatile IdleStrategy[] waiters;
    private final AtomicInteger waiting;
    private final ReplicationClock clock;

    public Followers(ReplicationClock clock) {
        this.connections = NONE;
        this.connectionList = List.of();
        this.waiters = NO_WAITERS;
        this.waiting = new AtomicInteger();
        this.clock = clock;
    }

    synchronized void add(FollowerConnection connection) {
        FollowerConnection[] added = Arrays.copyOf(connections, connections.length + 1);
        added[connections.length] = connection;
        setConnections(added);
    }

    synchronized void remove(FollowerConnection connection) {
        setConnections(Arrays.stream(connections).filter(other -> other != connection).toArray(FollowerConnection[]::new));
        wakeWaiters();
    }

    private void setConnections(FollowerConnection[] changed) {
        connections = changed;
        connectionList = List.of(changed);
    }

    /**
     * The strategy a stream's {@code SYNC} sends wait on, woken by {@link #wakeWaiters()}.
     */
    synchronized void addWaiter(IdleStrategy idle) {
        IdleStrategy[] added = Arrays.copyOf(waiters, waiters.length + 1);
        added[waiters.length] = idle;
        waiters = added;
    }

    synchronized void removeWaiter(IdleStrategy idle) {
        waiters = Arrays.stream(waiters).filter(other -> other != idle).toArray(IdleStrategy[]::new);
    }

    /**
     * After an ack, or a follower gone: wakes the {@code SYNC} sends parked waiting, only when one waits.
     */
    void wakeWaiters() {
        if (waiting.get() > 0) {
            for (IdleStrategy waiter : waiters) {
                waiter.wakeup();
            }
        }
    }

    /**
     * After a write to the log: wakes the senders parked waiting for one.
     */
    void wakeSenders() {
        for (FollowerConnection connection : connections) {
            connection.wakeSender();
        }
    }

    public List<FollowerConnection> connections() {
        return connectionList;
    }

    /**
     * Whether a follower holds every replicated stream: each of its lanes streaming and in sync, one lane in sync
     * not being enough. Any thread.
     *
     * @param streamCount the streams the leader replicates
     */
    public boolean isAnyFollowerInSync(int streamCount, int lanes) {
        return countFollowers(lanes, connection -> connection.isLaneInSync(streamCount)) > 0;
    }

    /**
     * The followers every lane of which has a connection matching, a follower with one lane down or behind not
     * counted. Any thread.
     */
    public int countFollowers(int lanes, Predicate<FollowerConnection> matching) {
        FollowerConnection[] current = connections;
        int count = 0;
        for (int i = 0; i < current.length; i++) {
            String followerId = current[i].getFollowerId();
            if (followerId != null && isFirstOf(current, i, followerId) && matchesEveryLane(current, followerId, lanes, matching)) {
                count++;
            }
        }
        return count;
    }

    private static boolean isFirstOf(FollowerConnection[] connections, int index, String followerId) {
        for (int i = 0; i < index; i++) {
            if (followerId.equals(connections[i].getFollowerId())) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesEveryLane(FollowerConnection[] connections, String followerId, int lanes,
                                            Predicate<FollowerConnection> matching) {
        for (int lane = 0; lane < lanes; lane++) {
            if (!matchesLane(connections, followerId, lane, matching)) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesLane(FollowerConnection[] connections, String followerId, int lane,
                                       Predicate<FollowerConnection> matching) {
        for (FollowerConnection connection : connections) {
            if (connection.getLane() == lane && followerId.equals(connection.getFollowerId()) && matching.test(connection)) {
                return true;
            }
        }
        return false;
    }

    /**
     * What the followers' sinks have yet to write, as of their last acks, over every lane. A closed connection is
     * left out: it stays listed until its CLOSE is flushed, possibly beside its follower's new one.
     */
    public long getPendingWrites() {
        long pendingWrites = 0;
        for (FollowerConnection connection : connections) {
            if (connection.isConnected()) {
                pendingWrites += connection.getAcks().getPendingWrites();
            }
        }
        return pendingWrites;
    }

    /**
     * Announces the stream again to every follower; see {@link org.lolaf.replicos.ReplicationNode#reannounce}.
     */
    public void reannounce(int streamKey) {
        for (FollowerConnection connection : connections) {
            connection.reannounce(streamKey);
        }
    }

    /**
     * Sends the stream again by snapshot to every follower holding a copy of it.
     */
    public void resync(int streamKey) {
        for (FollowerConnection connection : connections) {
            connection.resync(streamKey);
        }
    }

    /**
     * As {@link org.lolaf.replicos.StreamWriter#awaitAck}: a follower falling out of sync stops being waited for, and
     * with none left the wait ends at once. Allocates nothing.
     */
    Ack awaitAck(int streamKey, long offset, StreamReplicationMode mode, long timeoutNanos, IdleStrategy idle) {
        boolean received = mode == StreamReplicationMode.SYNC_RECEIVED;
        long startNanos = timeoutNanos == 0 ? 0 : clock.nanoTime();
        Ack ack = ackOf(streamKey, offset, received, timeoutNanos, startNanos);
        if (ack != null) {
            return ack;
        }
        idle.reset();
        waiting.incrementAndGet();
        try {
            while ((ack = ackOf(streamKey, offset, received, timeoutNanos, startNanos)) == null) {
                idle.idle();
            }
            return ack;
        } finally {
            waiting.decrementAndGet();
            idle.reset();
        }
    }

    /**
     * @return null while it waits on
     */
    private @Nullable Ack ackOf(int streamKey, long offset, boolean received, long timeoutNanos, long startNanos) {
        boolean anyInSync = false;
        boolean anyWaitedFor = false;
        boolean acked = true;
        for (FollowerConnection connection : connections) {
            if (connection.isStreamInSync(streamKey)) {
                anyInSync = true;
                if (!connection.getAcks().isOverdue()) {
                    anyWaitedFor = true;
                    acked &= ackedOffset(connection, received) >= offset;
                }
            }
        }
        if (!anyInSync) {
            return Ack.NO_FOLLOWER_IN_SYNC;
        }
        if (!anyWaitedFor) {
            return Ack.TIMED_OUT;
        }
        if (acked) {
            return Ack.ACKED;
        }
        if (timeoutNanos != 0 && clock.nanoTime() - startNanos >= timeoutNanos) {
            for (FollowerConnection connection : connections) {
                if (connection.isStreamInSync(streamKey) && ackedOffset(connection, received) < offset) {
                    connection.getAcks().overdue(offset);
                }
            }
            return Ack.TIMED_OUT;
        }
        return null;
    }

    /**
     * A read with no record of a {@code SYNC_RECEIVED} stream sends no {@code RECEIVED}, so the received offset can
     * trail the applied.
     */
    private static long ackedOffset(FollowerConnection connection, boolean received) {
        FollowerAcks acks = connection.getAcks();
        long applied = acks.getAppliedOffset();
        return received ? Math.max(acks.getReceivedOffset(), applied) : applied;
    }
}
