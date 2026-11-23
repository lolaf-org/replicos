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

import lombok.Getter;
import org.jspecify.annotations.Nullable;
import org.lolaf.replicos.spi.LatencyRecorder;
import org.lolaf.replicos.StreamWriter;
import org.lolaf.replicos.spi.ReplicationMetrics;
import org.lolaf.replicos.wire.CloseCounts;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What the leader's connections did over the node's life, across reconnections, read by the views on another thread:
 * {@link org.lolaf.replicos.FollowersView} documents each count.
 */
public final class ReplicationStats {

    private final AtomicLong droppedTooSlow;
    @Getter
    private final SlowFollowers slowFollowers;
    private final AtomicLong snapshots;
    private final AtomicLong snapshotRecords;
    private final AtomicLong sendWaits;
    private final AtomicLong sendBuffersAllocated;
    private final AtomicLong logFullWaits;
    @Getter
    private final CloseCounts closes;
    private final Map<Integer, AckCounts> acksByStream;
    private final ReplicationMetrics metrics;

    public ReplicationStats(SlowFollowers slowFollowers, ReplicationMetrics metrics) {
        this.slowFollowers = slowFollowers;
        this.metrics = metrics;
        this.droppedTooSlow = new AtomicLong();
        this.snapshots = new AtomicLong();
        this.snapshotRecords = new AtomicLong();
        this.sendWaits = new AtomicLong();
        this.sendBuffersAllocated = new AtomicLong();
        this.logFullWaits = new AtomicLong();
        this.closes = new CloseCounts();
        this.acksByStream = new ConcurrentHashMap<>();
    }

    LatencyRecorder ackLatency(String followerId, int lane) {
        return metrics.followerAckLatency(followerId, lane);
    }

    LatencyRecorder lag(String followerId, int lane) {
        return metrics.followerLag(followerId, lane);
    }

    LatencyRecorder catchUp(String followerId, int lane) {
        return metrics.followerCatchUp(followerId, lane);
    }

    void logFullWaited() {
        logFullWaits.incrementAndGet();
    }

    /**
     * The stream's ack results, for its writers to count into; allocates the first time.
     */
    AckCounts acks(int streamKey) {
        return acksByStream.computeIfAbsent(streamKey, key -> new AckCounts());
    }

    void droppedTooSlow(@Nullable String followerId, int lane) {
        droppedTooSlow.incrementAndGet();
        if (followerId != null) {
            slowFollowers.dropped(followerId, lane);
        }
    }

    void snapshotSent(int records) {
        snapshots.incrementAndGet();
        snapshotRecords.addAndGet(records);
    }

    void sendWaited() {
        sendWaits.incrementAndGet();
    }

    void sendBuffersAllocated(int count) {
        sendBuffersAllocated.addAndGet(count);
    }

    public long getSendWaits() {
        return sendWaits.get();
    }

    public long getSendBuffersAllocated() {
        return sendBuffersAllocated.get();
    }

    public long getDroppedTooSlow() {
        return droppedTooSlow.get();
    }

    public long getLogFullWaits() {
        return logFullWaits.get();
    }

    public long getAckTimeouts(int streamKey) {
        AckCounts counts = acksByStream.get(streamKey);
        return counts == null ? 0 : counts.timedOut.get();
    }

    public long getAcksWithNoFollowerInSync(int streamKey) {
        AckCounts counts = acksByStream.get(streamKey);
        return counts == null ? 0 : counts.noFollowerInSync.get();
    }

    public long getSnapshots() {
        return snapshots.get();
    }

    public long getSnapshotRecords() {
        return snapshotRecords.get();
    }

    static final class AckCounts {

        private final AtomicLong timedOut;
        private final AtomicLong noFollowerInSync;

        AckCounts() {
            this.timedOut = new AtomicLong();
            this.noFollowerInSync = new AtomicLong();
        }

        void count(StreamWriter.Ack ack) {
            if (ack == StreamWriter.Ack.TIMED_OUT) {
                timedOut.incrementAndGet();
            } else if (ack == StreamWriter.Ack.NO_FOLLOWER_IN_SYNC) {
                noFollowerInSync.incrementAndGet();
            }
        }
    }
}
