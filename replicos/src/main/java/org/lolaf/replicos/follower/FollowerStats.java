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
package org.lolaf.replicos.follower;

import lombok.Getter;
import org.lolaf.replicos.wire.CloseCounts;

import java.util.concurrent.atomic.AtomicLong;

/**
 * What the follower's lanes did over the node's life, across leaders and reconnections, read on another thread.
 */
public final class FollowerStats {

    private final AtomicLong reconnects;
    private final AtomicLong bytesReceived;
    private final AtomicLong recordsReceived;
    private final AtomicLong snapshots;
    private final AtomicLong snapshotRecords;
    @Getter
    private final CloseCounts closes;

    FollowerStats() {
        this.reconnects = new AtomicLong();
        this.bytesReceived = new AtomicLong();
        this.recordsReceived = new AtomicLong();
        this.snapshots = new AtomicLong();
        this.snapshotRecords = new AtomicLong();
        this.closes = new CloseCounts();
    }

    void reconnected() {
        reconnects.incrementAndGet();
    }

    void received(int bytes, int records) {
        bytesReceived.addAndGet(bytes);
        recordsReceived.addAndGet(records);
    }

    void snapshotReceived(int records) {
        snapshots.incrementAndGet();
        snapshotRecords.addAndGet(records);
    }

    public long getReconnects() {
        return reconnects.get();
    }

    public long getBytesReceived() {
        return bytesReceived.get();
    }

    public long getRecordsReceived() {
        return recordsReceived.get();
    }

    public long getSnapshots() {
        return snapshots.get();
    }

    public long getSnapshotRecords() {
        return snapshotRecords.get();
    }
}
