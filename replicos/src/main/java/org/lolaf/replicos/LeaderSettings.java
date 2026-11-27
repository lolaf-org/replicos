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

import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import org.lolaf.replicos.spi.LeaderStreams;
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;
import java.util.function.Supplier;

/**
 * What a node reads while it leads. The bind address and the streams are required; everything else has a default.
 */
@Value
@Builder
public class LeaderSettings {

    /**
     * Where the node accepts its followers.
     */
    @NonNull InetSocketAddress bindAddress;
    /**
     * What the node replicates.
     */
    @NonNull LeaderStreams streams;
    /**
     * How many lanes the streams are split over, each with its own log, sender thread and connection per follower: a
     * snapshot, or a follower too slow for one lane, then holds back only that lane's streams, and the follower applies
     * the lanes in parallel. The leader tells its followers.
     */
    @Builder.Default
    int lanes = 1;
    /**
     * How long an append waits for a follower a whole log behind before the follower is dropped as too slow, and
     * caught up by snapshot. The wait holds the appending thread, so it stays short; positive, since a log that never
     * waits would lap its followers.
     */
    @Builder.Default
    @NonNull Duration logFullTimeout = Duration.ofSeconds(1);
    /**
     * How long a follower dropped as too slow is refused before it may resync, since catching it up adds to the load
     * that made it too slow; doubled at each drop up to a minute. Zero never holds a follower off.
     */
    @Builder.Default
    @NonNull Duration slowFollowerHoldOff = Duration.ofSeconds(1);
    /**
     * How often a leader with nothing to send tells its followers where the log ends, so they can tell an idle leader
     * from a dead connection.
     */
    @Builder.Default
    @NonNull Duration heartbeatInterval = Duration.ofSeconds(1);
    /**
     * How long a follower owing an ack may stay silent before the leader drops it as hung, and how long an encrypting
     * follower may take to confirm its keys. Zero never drops.
     */
    @Builder.Default
    @NonNull Duration followerStallTimeout = Duration.ofSeconds(10);
    /**
     * The send buffers per follower lane, a power of two of at least 2; with all of them in flight the sender waits
     * for one. More help a follower far away.
     */
    @Builder.Default
    int sendBuffers = 4;
    /**
     * The most bytes of a snapshot sent between two batches of the lane's other records: larger finishes a snapshot
     * sooner under load, smaller delays the other streams less.
     */
    @Builder.Default
    int snapshotChunkBytes = 64 << 10;
    /**
     * How the sender, a thread per follower lane, waits for records to send, woken by an append. A strategy that
     * ignores {@code wakeup()} parks its whole period, which an ack wait then waits on.
     */
    @Builder.Default
    @NonNull Supplier<IdleStrategy> senderIdleStrategy = BackoffIdleStrategy::new;
    /**
     * How a {@link StreamWriter} waits for its followers' acks, on the appending thread, woken by the acks.
     */
    @Builder.Default
    @NonNull Supplier<IdleStrategy> ackIdleStrategy = BackoffIdleStrategy::new;
    /**
     * The node ids of the followers a {@code SYNC} append waits for, the others replicating as if {@code ASYNC}: a
     * failover to one of these loses nothing acknowledged. Empty, the default, makes every follower one. This node's
     * own id may be in it, so every node can be given the same ids.
     */
    @Singular
    Set<String> syncFollowers;
}
