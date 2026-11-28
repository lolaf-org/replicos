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
import lombok.NonNull;
import lombok.Value;
import org.jspecify.annotations.Nullable;
import org.lolaf.replicos.RegisteredStream;
import org.lolaf.replicos.StreamSettings;
import org.lolaf.replicos.StreamWriter;
import org.lolaf.replicos.spi.SnapshotSource;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A stream at the key {@link StreamRegistry} gave it: the application's handle, and what the leader's senders
 * announce and snapshot.
 */
public final class StreamRegistration implements RegisteredStream {

    @Getter
    private final int key;
    @Getter
    private final int lane;
    @Getter
    private final String streamName;
    private final Supplier<SnapshotSource> snapshotSource;
    private final StreamWriters writers;
    private final Followers followers;
    private final ReplicationStats.AckCounts acks;
    /**
     * Replaced whole, so the announcer reads a mode and an identity set together.
     */
    @Getter
    private volatile Replication replication;

    StreamRegistration(int key, int lane, StreamSettings settings, StreamWriters writers, Followers followers,
                       ReplicationStats stats) {
        this.key = key;
        this.lane = lane;
        this.streamName = settings.getName();
        this.replication = new Replication(settings.getReplicationMode(), settings.getStorageIdentity());
        this.snapshotSource = settings.getSnapshotSource();
        this.writers = writers;
        this.followers = followers;
        this.acks = stats.acks(key);
    }

    @Override
    public StreamReplicationMode getReplicationMode() {
        return replication.getMode();
    }

    @Override
    public @Nullable String getStorageIdentity() {
        return replication.getStorageIdentity();
    }

    /**
     * Any mode but {@link StreamReplicationMode#NONE}.
     */
    public boolean isReplicated() {
        return replication.getMode() != StreamReplicationMode.NONE;
    }

    public SnapshotSource newSnapshotSource() {
        return snapshotSource.get();
    }

    @Override
    public synchronized void reconfigure(StreamReplicationMode replicationMode, @Nullable String storageIdentity) {
        Replication next = new Replication(replicationMode, storageIdentity);
        if (next.equals(replication)) {
            return;
        }
        replication = next;
        followers.reannounce(key);
    }

    @Override
    public StreamWriter writer() {
        return writers.open(key);
    }

    @Override
    public void resync() {
        followers.resync(key);
    }

    @Override
    public boolean isInSync() {
        for (FollowerConnection connection : followers.connections()) {
            if (connection.isSyncFollowerInSync(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Map<String, StreamAnswer> getRefusals() {
        Map<String, StreamAnswer> refusals = Map.of();
        for (FollowerConnection connection : followers.connections()) {
            StreamAnswer answer = connection.getStreamAnswer(key);
            String followerId = connection.getFollowerId();
            if (answer != null && answer.isRefusal() && followerId != null) {
                if (refusals.isEmpty()) {
                    refusals = new HashMap<>();
                }
                refusals.put(followerId, answer);
            }
        }
        return refusals;
    }

    @Override
    public long getAckTimeouts() {
        return acks.getTimedOut();
    }

    @Override
    public long getAcksWithNoFollowerInSync() {
        return acks.getNoFollowerInSync();
    }

    @Override
    public long getAcksWithNotEnoughFollowers() {
        return acks.getNotEnoughFollowers();
    }

    @Override
    public String toString() {
        return streamName;
    }

    @Value
    public static class Replication {

        @NonNull
        StreamReplicationMode mode;
        @Nullable String storageIdentity;
    }
}
