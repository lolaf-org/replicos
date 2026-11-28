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
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.util.Map;

/**
 * A stream the node replicates while it leads, as {@link ReplicationNode#register} returned it: what the application
 * writes and checks the stream through. Any thread.
 */
public interface RegisteredStream {

    String getStreamName();

    /**
     * The lane the stream's records go through, the streams being spread over the lanes in registration order.
     */
    int getLane();

    StreamReplicationMode getReplicationMode();

    /**
     * {@link #reconfigure} with the storage identity kept.
     */
    void setReplicationMode(StreamReplicationMode replicationMode);

    /**
     * Null when the leader cannot tell where it stores the stream.
     */
    @Nullable String getStorageIdentity();

    /**
     * {@link #reconfigure} with the replication mode kept.
     */
    void setStorageIdentity(@Nullable String storageIdentity);

    /**
     * Opens a writer, for one thread at a time. Its records reach the followers while the node leads.
     */
    StreamWriter writer();

    /**
     * Announces the stream again, unless both are unchanged: each follower checks it against its own settings and
     * keeps its copy when it still can, catches up by snapshot when it now needs one, or stops replicating it. Both
     * change at once, so no follower is told one without the other.
     */
    void reconfigure(StreamReplicationMode replicationMode, @Nullable String storageIdentity);

    /**
     * Call after the stream's data changed without records appended, such as a restore or a reset of its storage:
     * every follower holding the stream in sync discards its copy and receives the whole stream again by snapshot.
     * Followers not holding it are left alone.
     */
    void resync();

    /**
     * A {@link LeaderSettings#getSyncFollowers() sync follower} holds the stream in sync, so a failover to it loses
     * none of the stream's acknowledged records. False while the node does not lead.
     */
    boolean isInSync();

    /**
     * The followers refusing the stream, by node id, and why. Empty while the node does not lead.
     */
    Map<String, StreamAnswer> getRefusals();

    /**
     * Ack waits on the stream that timed out, over the node's life.
     */
    long getAckTimeouts();

    /**
     * Ack waits on the stream that found no follower in sync, over the node's life.
     */
    long getAcksWithNoFollowerInSync();

    /**
     * Ack waits on the stream that found fewer followers in sync than their quorum, over the node's life.
     */
    long getAcksWithNotEnoughFollowers();
}
