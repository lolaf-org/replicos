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

import java.util.List;
import java.util.Map;

/**
 * The leader of a following node, and what this node holds of the leader's streams. Any thread.
 */
public interface LeaderView {

    /**
     * Null while the node does not follow.
     */
    @Nullable LeaderStatus getStatus();

    /**
     * Every lane the leader announced is streaming; false while the node does not follow.
     */
    boolean isConnected();

    /**
     * Connected, and holding every stream the leader replicates in sync.
     */
    boolean isInSync();

    /**
     * Every lane the leader announced is streaming encrypted.
     */
    boolean isEncrypted();

    /**
     * Every stream the leader replicates is in sync here, connected or not: what a node about to lead holds. One
     * refused is missing; a node that never followed misses nothing.
     */
    boolean isCopyInSync();

    /**
     * The leader's lane count, from its last WELCOME, 1 before any.
     */
    int getLanes();

    /**
     * The last record this node wrote on each lane as a follower, kept across connections: what a failover to it
     * keeps, to compare with another follower's before promoting one that is not a sync follower.
     */
    List<LanePosition> getLanePositions();

    /**
     * Null for a stream the leader never announced, or once the node led.
     *
     * @param streamName as the leader announces it, the same on every node
     */
    @Nullable ReplicaStream getStream(String streamName);

    /**
     * Lane connections made again to a leader after one dropped, over the node's life.
     */
    long getReconnects();

    /**
     * Bytes of frames received from leaders, over the node's life.
     */
    long getBytesReceived();

    /**
     * Records received from leaders outside snapshots, over the node's life.
     */
    long getRecordsReceived();

    /**
     * Stream snapshots received to catch up, over the node's life.
     */
    long getSnapshots();

    /**
     * Records the snapshots copied, over the node's life.
     */
    long getSnapshotRecords();

    /**
     * Lane connections to a leader closed over the node's life, by the close reason of either side as
     * {@code docs/protocol.md} names it, or {@code DISCONNECTED} for a connection lost without one. Only the reasons
     * that happened.
     */
    Map<String, Long> getCloses();
}
