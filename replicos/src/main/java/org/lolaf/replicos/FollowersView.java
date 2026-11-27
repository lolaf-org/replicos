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

import org.lolaf.replicos.spi.StreamAnswer;

import java.util.List;
import java.util.Map;

/**
 * The followers of a leading node, empty while it does not lead. A follower counts once all its lanes match, one lane
 * down or behind being enough to leave it out. Any thread.
 */
public interface FollowersView {

    /**
     * Every follower lane connected, streaming or not yet.
     */
    List<FollowerLaneStatus> getLaneStatuses();

    /**
     * The follower lanes dropped as too slow, for a write waited the log full timeout on them, and not back in sync
     * since.
     */
    List<DroppedFollower> getDroppedFollowers();

    /**
     * The followers with every lane connected.
     */
    int countConnected();

    /**
     * The followers holding every replicated stream in sync, each a node a failover may go to without losing what
     * was acknowledged.
     */
    int countInSync();

    /**
     * As {@link #countInSync()} above 0, without counting them all.
     */
    boolean isAnyInSync();

    /**
     * What the followers' sinks have yet to write, as of their last acks.
     */
    long getPendingWrites();

    /**
     * The most bytes of the log a follower lane has not been sent yet, 0 for none.
     */
    long getMaxUnsentBytes();

    /**
     * The longest a follower lane has been behind a record it has not applied yet, 0 for none.
     */
    long getMaxLagNanos();

    /**
     * A follower holds the stream in sync, so a failover to it loses none of the stream's acknowledged records.
     *
     * @param streamKey the stream's {@link org.lolaf.replicos.spi.LeaderStream#getKey() key}
     */
    boolean isStreamInSync(int streamKey);

    /**
     * The followers refusing the stream, by node id, and why.
     *
     * @param streamKey the stream's {@link org.lolaf.replicos.spi.LeaderStream#getKey() key}
     */
    Map<String, StreamAnswer> getStreamRefusals(int streamKey);

    /**
     * Follower lanes dropped as too slow, over the node's life.
     */
    long getDroppedTooSlow();

    /**
     * Snapshots sent to follower lanes catching up, over the node's life.
     */
    long getSnapshots();

    /**
     * Records the snapshots copied, over the node's life.
     */
    long getSnapshotRecords();

    /**
     * Times a sender had something to send and could not: every send buffer in flight, or the socket full.
     */
    long getSendWaits();

    /**
     * Send buffers allocated for want of a pooled one, 0 unless the senders' wait is defeated.
     */
    long getSendBuffersAllocated();

    /**
     * Appends that found the log full and waited for a slow follower, over the node's life: a follower falling behind
     * before the log full timeout drops it.
     */
    long getLogFullWaits();

    /**
     * Follower lanes closed over the node's life, by the close reason of either side as {@code docs/protocol.md} names
     * it, or {@code DISCONNECTED} for a connection lost without one. Only the reasons that happened.
     */
    Map<String, Long> getCloses();

    /**
     * Ack waits on the stream that timed out, over the node's life.
     *
     * @param streamKey the stream's {@link org.lolaf.replicos.spi.LeaderStream#getKey() key}
     */
    long getAckTimeouts(int streamKey);

    /**
     * Ack waits on the stream that found no follower in sync, over the node's life.
     *
     * @param streamKey the stream's {@link org.lolaf.replicos.spi.LeaderStream#getKey() key}
     */
    long getAcksWithNoFollowerInSync(int streamKey);

    /**
     * Ack waits on the stream that found fewer followers in sync than their quorum, over the node's life.
     *
     * @param streamKey the stream's {@link org.lolaf.replicos.spi.LeaderStream#getKey() key}
     */
    long getAcksWithNotEnoughFollowers(int streamKey);
}
