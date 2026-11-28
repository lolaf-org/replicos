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
import org.lolaf.ringos.Deadline;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * One node of a replicated application: it leads, streaming its streams' records to its followers, or follows,
 * applying its leader's into its sinks. Which, and when, is for the application's leader election to say: the role
 * methods are called from one thread, the rest from any.
 */
public interface ReplicationNode {

    /**
     * A node needs its id and the settings of each role it may take.
     */
    static ReplicationNodeBuilder builder() {
        return new ReplicationNodeBuilder();
    }

    /**
     * Stands down, stops the sinks of what the node held as a follower, so the application can take over its streams,
     * then accepts followers on the bind address. Each call is a new leadership: a follower of an earlier one, even at
     * the same epoch, catches up by snapshot rather than from the log.
     *
     * @param epoch    the leadership this node holds, sent with every frame so its followers refuse a deposed leader
     * @param deadline for standing down and stopping the sinks
     */
    void lead(long epoch, Deadline deadline);

    /**
     * Stands down, then connects to the leader, resuming from what the node holds of its streams.
     *
     * @param leaderAddresses the leader's, each lane connecting through the first that answers: a leader reachable
     *                        over several networks. Every lane must reach the same leader; a lane reaching another is
     *                        refused, and tries again.
     * @param epoch           the leadership the node knows of, read on every connection
     * @param deadline        for standing down
     * @throws IllegalArgumentException for no address
     */
    void follow(List<InetSocketAddress> leaderAddresses, LongSupplier epoch, Deadline deadline);

    /**
     * Follows a leader at one address.
     *
     * @param leaderAddress the leader's, every lane connecting through it
     * @param epoch         as {@link #follow(List, LongSupplier, Deadline)}
     * @param deadline      for standing down
     */
    default void follow(InetSocketAddress leaderAddress, LongSupplier epoch, Deadline deadline) {
        follow(List.of(leaderAddress), epoch, deadline);
    }

    /**
     * For a leader stopping cleanly, once the application stopped writing: sends each follower the rest of the log,
     * then tells it this leader stops, so the follower promoted next knows it lost no write
     * ({@link ReplicaStream#isHandedOver()}). A follower not caught up by the deadline is disconnected without being
     * told. Once one was told, new followers are refused. The node still leads: {@link #standDown} or {@link #close}
     * comes next.
     *
     * @param deadline for every follower to be sent the rest of the log
     * @return whether at least one follower was told; false when not leading
     */
    boolean handOver(Deadline deadline);

    /**
     * Stops leading or following. A leader disconnects its followers without telling them, unlike
     * {@link #handOver}. A follower waits, within the deadline, for its sinks to write what it received; the sinks
     * stay open and the node keeps where each stream stands, so following the same leader again resumes rather than
     * starting over by snapshot.
     *
     * @param deadline for a leader's connections to close, and a follower's sinks to write what it received
     */
    void standDown(Deadline deadline);

    /**
     * Stands down, then stops every sink, for a node shutting down.
     *
     * @param deadline for standing down, then for each sink to stop
     */
    void close(Deadline deadline);

    /**
     * From {@link #lead} until the node stands down or closes.
     */
    boolean isLeading();

    /**
     * From {@link #follow} until the node stands down or closes, connected to the leader or not.
     */
    boolean isFollowing();

    /**
     * Replicates the stream from now on, announcing it to the followers connected or connecting. Any thread, leading
     * or not.
     *
     * @throws IllegalArgumentException for a name empty, longer than 65535 UTF-8 bytes, or registered already
     * @throws IllegalStateException    for a node without leader settings
     */
    RegisteredStream register(StreamSettings settings);

    /**
     * Null for a name never registered.
     */
    @Nullable RegisteredStream stream(String streamName);

    /**
     * In registration order.
     */
    List<RegisteredStream> streams();

    /**
     * The followers while the node leads, empty otherwise.
     */
    FollowersView followers();

    /**
     * The leader while the node follows, and what the node holds of its streams, kept until it leads.
     */
    LeaderView leader();

    /**
     * The leader's lanes, each a log and a connection per follower, so a snapshot or a slow follower on one lane
     * holds back only its streams. A stream's is {@link RegisteredStream#getLane()}.
     */
    int getLanes();

    /**
     * The bytes written to every lane's log since the node was built.
     */
    long getLogBytesWritten();
}
