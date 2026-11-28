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

import lombok.RequiredArgsConstructor;
import org.lolaf.replicos.leader.FollowerConnection;
import org.lolaf.replicos.leader.Followers;
import org.lolaf.replicos.leader.ReplicationStats;
import org.lolaf.replicos.leader.StreamRegistry;
import org.lolaf.replicos.log.ReplicationLogs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
final class FollowersViewImpl implements FollowersView {

    private final Followers followers;
    private final ReplicationStats stats;
    private final ReplicationLogs logs;
    private final StreamRegistry streams;

    @Override
    public List<FollowerLaneStatus> getLaneStatuses() {
        List<FollowerConnection> connections = followers.connections();
        int streamCount = streams.count();
        List<FollowerLaneStatus> statuses = new ArrayList<>(connections.size());
        for (FollowerConnection connection : connections) {
            statuses.add(connection.getStatus(streamCount));
        }
        return statuses;
    }

    @Override
    public List<DroppedFollower> getDroppedFollowers() {
        return stats.getSlowFollowers().notBackInSync();
    }

    @Override
    public int countConnected() {
        return followers.countFollowers(logs.count(), FollowerConnection::isConnected);
    }

    @Override
    public int countInSync() {
        int streamCount = streams.count();
        return followers.countFollowers(logs.count(), connection -> connection.isSyncFollowerLaneInSync(streamCount));
    }

    @Override
    public boolean isAnyInSync() {
        return followers.isAnySyncFollowerInSync(streams.count(), logs.count());
    }

    @Override
    public long getPendingWrites() {
        return followers.getPendingWrites();
    }

    @Override
    public long getMaxUnsentBytes() {
        long most = 0;
        for (FollowerConnection connection : followers.connections()) {
            most = Math.max(most, connection.getUnsentBytes());
        }
        return most;
    }

    @Override
    public long getMaxLagNanos() {
        long most = 0;
        for (FollowerConnection connection : followers.connections()) {
            most = Math.max(most, connection.getLagNanos());
        }
        return most;
    }

    @Override
    public long getTooSlowDrops() {
        return stats.getTooSlowDrops();
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
    public long getSendWaits() {
        return stats.getSendWaits();
    }

    @Override
    public long getSendBuffersAllocated() {
        return stats.getSendBuffersAllocated();
    }

    @Override
    public long getLogFullWaits() {
        return stats.getLogFullWaits();
    }

    @Override
    public Map<String, Long> getCloses() {
        return stats.getCloses().toMap();
    }
}
