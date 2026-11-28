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
import org.jspecify.annotations.Nullable;
import org.lolaf.replicos.spi.SnapshotSource;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.util.function.Supplier;

/**
 * A stream to {@link ReplicationNode#register register}. Everything but the storage identity is required.
 */
@Value
@Builder
public class StreamSettings {

    /**
     * What a follower finds its own copy of the stream by, unique to the node.
     */
    @NonNull
    String name;
    /**
     * What the stream's appends wait for from the followers; {@link StreamReplicationMode#NONE} replicates it not at
     * all. {@link RegisteredStream#reconfigure} changes it.
     */
    @NonNull
    StreamReplicationMode replicationMode;
    /**
     * Where the leader stores the stream, so a follower storing into the same place refuses it: replicating into it
     * would rewrite the leader's data. Absent when the leader cannot tell: each follower's sink decides, refusing the
     * stream as {@link org.lolaf.replicos.spi.StreamAnswer#UNKNOWN_STORAGE} unless its own storage cannot be the
     * leader's. {@link RegisteredStream#reconfigure} changes it.
     */
    @Nullable String storageIdentity;
    /**
     * Asked for a source as each snapshot of the stream starts, on the sender thread of the lane that needs it.
     */
    @NonNull
    Supplier<SnapshotSource> snapshotSource;
}
