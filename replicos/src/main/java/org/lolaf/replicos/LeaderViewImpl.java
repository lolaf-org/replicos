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
import org.lolaf.replicos.follower.FollowerStats;
import org.lolaf.replicos.follower.FollowerStreams;
import org.lolaf.replicos.follower.ReplicationClient;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

final class LeaderViewImpl implements LeaderView {

    private final Supplier<@Nullable ReplicationClient> client;
    private final FollowerStreams followerStreams;
    private final FollowerStats stats;

    LeaderViewImpl(Supplier<@Nullable ReplicationClient> client, FollowerStreams followerStreams) {
        this.client = client;
        this.followerStreams = followerStreams;
        this.stats = followerStreams.getStats();
    }

    @Override
    public @Nullable LeaderStatus getStatus() {
        ReplicationClient following = client.get();
        return following == null ? null : following.getStatus();
    }

    @Override
    public boolean isConnected() {
        ReplicationClient following = client.get();
        return following != null && following.isConnected();
    }

    @Override
    public boolean isInSync() {
        ReplicationClient following = client.get();
        return following != null && following.isInSync();
    }

    @Override
    public boolean isEncrypted() {
        ReplicationClient following = client.get();
        return following != null && following.isEncrypted();
    }

    @Override
    public boolean isCopyInSync() {
        return followerStreams.isInSync();
    }

    @Override
    public int getLanes() {
        return followerStreams.getLanes();
    }

    @Override
    public List<LanePosition> getLanePositions() {
        return followerStreams.getLanePositions();
    }

    @Override
    public @Nullable ReplicaStream getStream(String streamName) {
        return followerStreams.find(streamName);
    }

    @Override
    public long getReconnects() {
        return stats.getReconnects();
    }

    @Override
    public long getBytesReceived() {
        return stats.getBytesReceived();
    }

    @Override
    public long getRecordsReceived() {
        return stats.getRecordsReceived();
    }

    @Override
    public long getSnapshots() {
        return stats.getSnapshots();
    }

    @Override
    public long getSnapshotRecords() {
        return stats.getSnapshotRecords();
    }

    @Override
    public Map<String, Long> getCloses() {
        return stats.getCloses().toMap();
    }
}
