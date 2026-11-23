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
import lombok.Value;
import org.lolaf.replicos.spi.ReplicaSinks;
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * What a node reads while it follows. The sinks are required; everything else has a default.
 */
@Value
@Builder
public class FollowerSettings {

    /**
     * Where the node applies its leader's streams.
     */
    @NonNull ReplicaSinks sinks;
    /**
     * How long the follower waits between two attempts to connect to its leader.
     */
    @Builder.Default
    @NonNull Duration reconnectInterval = Duration.ofSeconds(1);
    /**
     * How often the follower checks again its sinks' pending writes, which it reports to its leader, and whether a
     * stream refused until its configuration changes may be opened now.
     */
    @Builder.Default
    @NonNull Duration checkInterval = Duration.ofSeconds(1);
    /**
     * Threads writing what the follower receives into its sinks, its lanes spread over them; 0 writes on each lane's
     * network thread. With writer threads the network thread keeps reading while a sink writes, so a
     * {@code SYNC_RECEIVED} record is acknowledged without waiting for the write before it.
     */
    @Builder.Default
    int sinkWriterThreads = 0;
    /**
     * How a sink writer thread waits for what to write, woken by the network thread that read it.
     */
    @Builder.Default
    @NonNull Supplier<IdleStrategy> sinkWriterIdleStrategy = BackoffIdleStrategy::new;
}
