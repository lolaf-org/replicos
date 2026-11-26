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
package org.lolaf.replicos.examples.keyvalue;

import lombok.extern.slf4j.Slf4j;
import org.lolaf.replicos.spi.ReplicaSink;
import org.lolaf.ringos.Deadline;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Applies each change as it arrives: the store being in memory, there is nothing left to write once applied. Reads its
 * last changes back for {@link org.lolaf.replicos.CatchUp#byLastRecords()}, so a snapshot copies only what follows
 * them.
 */
@Slf4j
final class KeyValueSink implements ReplicaSink {

    private final KeyValueStore store;

    KeyValueSink(KeyValueStore store) {
        this.store = store;
    }

    @Override
    public void apply(byte contentType, long position, ByteBuffer buffer, int payloadIndex, int payloadLength) {
        Change change = Change.decode(contentType, buffer, payloadIndex, payloadLength);
        store.apply(position, change);
        log.info("Applied {} at {}", change, position);
    }

    @Override
    public void readLast(int records, RecordConsumer consumer) {
        for (Map.Entry<Long, Change> entry : store.lastChanges(records)) {
            Change change = entry.getValue();
            ByteBuffer payload = change.encode();
            if (!consumer.onRecord(change.getKind(), entry.getKey(), payload, 0, payload.limit())) {
                return;
            }
        }
    }

    @Override
    public void snapshotBegin(long fromPosition, ByteBuffer buffer, int stateIndex, int stateLength) {
        store.truncate(fromPosition);
        log.info("Snapshot started from position {}, the store kept before it", fromPosition);
    }

    @Override
    public void snapshotRecord(byte contentType, long position, ByteBuffer buffer, int payloadIndex, int payloadLength) {
        store.apply(position, Change.decode(contentType, buffer, payloadIndex, payloadLength));
    }

    @Override
    public void snapshotEnd(ByteBuffer buffer, int stateIndex, int stateLength) {
        log.info("Snapshot done: {}", store.entries());
    }

    @Override
    public void writePending() {
        // applied in memory already
    }

    @Override
    public void discardPending() {
        // nothing pending
    }

    @Override
    public long getPendingWrites() {
        return 0;
    }

    @Override
    public void stop(Deadline deadline) {
        // nothing to release
    }
}
