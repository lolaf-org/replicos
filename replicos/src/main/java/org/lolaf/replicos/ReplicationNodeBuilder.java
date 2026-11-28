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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.jspecify.annotations.Nullable;
import org.lolaf.betty.api.ss.SelectStrategy;
import org.lolaf.betty.api.ss.WakeupSelectStrategy;
import org.lolaf.replicos.spi.ReplicationClock;
import org.lolaf.replicos.spi.ReplicationMetrics;

import java.util.function.Supplier;

/**
 * Builds a {@link ReplicationNode}: what both roles read here, and each role's own settings. The node id is required,
 * and the settings of at least one role; a node without a role's settings refuses to take that role.
 */
@Getter(AccessLevel.PACKAGE)
@Setter
@Accessors(fluent = true, chain = true)
public final class ReplicationNodeBuilder {

    /**
     * Names this node to its peers in the handshake, and in their logs.
     */
    private String nodeId;
    /**
     * The replication cluster the node belongs to, the same on its leader and followers: a peer of another cluster,
     * such as another application's or another environment's, is refused.
     */
    private String cluster;
    /**
     * Needed for the node to lead.
     */
    private @Nullable LeaderSettings leader;
    /**
     * Needed for the node to follow.
     */
    private @Nullable FollowerSettings follower;
    /**
     * Bytes of off-heap memory of each lane's log, a power of two. A sync follower this far behind makes appends wait
     * for it, at most {@link LeaderSettings#getLogFullTimeout()}. The longest payload is half of it, on the leader and
     * on a follower alike, so every node takes the same. 16 MiB by default.
     */
    private int logCapacity;
    /**
     * Encrypts the replication with a secret the nodes share; off by default.
     */
    private ReplicationEncryptionSettings encryption;
    /**
     * What a follower holding part of a stream is sent to catch up; the same on every node.
     * {@link CatchUp#fullCopy()} by default.
     */
    private CatchUp catchUp;
    /**
     * How the network threads wait, the leader's and the follower's.
     */
    private Supplier<SelectStrategy> selectStrategy;
    private ReplicationClock clock;
    private ReplicationMetrics metrics;

    ReplicationNodeBuilder() {
        this.logCapacity = 16 << 20;
        this.encryption = ReplicationEncryptionSettings.builder().build();
        this.catchUp = CatchUp.fullCopy();
        this.selectStrategy = () -> new WakeupSelectStrategy(10);
        this.clock = ReplicationClock.SYSTEM;
        this.metrics = ReplicationMetrics.NONE;
    }

    /**
     * @throws IllegalStateException without a node id or a cluster, or without the settings of either role
     */
    public ReplicationNode build() {
        if (nodeId == null) {
            throw new IllegalStateException("nodeId is required");
        }
        if (cluster == null || cluster.isEmpty()) {
            throw new IllegalStateException("cluster is required");
        }
        if (leader == null && follower == null) {
            throw new IllegalStateException("leader or follower settings are required");
        }
        return new ReplicationNodeImpl(this);
    }
}
