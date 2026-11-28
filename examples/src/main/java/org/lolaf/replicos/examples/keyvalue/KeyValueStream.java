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

import lombok.RequiredArgsConstructor;
import org.lolaf.replicos.AckWait;
import org.lolaf.replicos.StreamSettings;
import org.lolaf.replicos.StreamWriter;
import org.lolaf.replicos.spi.RecordFlag;
import org.lolaf.replicos.spi.SnapshotSource;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

/**
 * The store's changes as the one stream the leader replicates, each acknowledged once a follower applied it.
 */
@RequiredArgsConstructor
final class KeyValueStream {

    static final String NAME = "key-values";
    static final StreamReplicationMode MODE = StreamReplicationMode.SYNC_APPLIED;
    private static final AckWait ACK_WAIT = AckWait.of(MODE, Duration.ofSeconds(1));

    private final KeyValueStore store;
    private final StreamWriter writer;

    static StreamSettings settings(KeyValueStore store, String storageIdentity) {
        return StreamSettings.builder()
                .name(NAME)
                .replicationMode(MODE)
                .storageIdentity(storageIdentity)
                .snapshotSource(() -> new JournalSnapshot(store))
                .build();
    }

    /**
     * Stores the change, then appends it, in the order replicos asks; called from one thread only.
     */
    StreamWriter.Ack write(Change change) {
        long position = store.lastPosition() + 1;
        store.apply(position, change);
        ByteBuffer payload = change.encode();
        long offset = writer.append(change.getKind(), RecordFlag.NONE, position, payload, 0, payload.limit());
        return writer.awaitAck(offset, ACK_WAIT);
    }

    /**
     * Copies the journal: a follower replays it from the first change.
     */
    @RequiredArgsConstructor
    private static final class JournalSnapshot implements SnapshotSource {

        private final KeyValueStore store;

        @Override
        public boolean isReadable() {
            return true;
        }

        @Override
        public long captureState(ByteBuffer state) {
            long lastPosition = store.lastPosition();
            state.putLong(lastPosition);
            return lastPosition;
        }

        @Override
        public void read(long fromPosition, long toPosition, RecordConsumer consumer) {
            for (Map.Entry<Long, Change> entry : store.changes(fromPosition, toPosition)) {
                Change change = entry.getValue();
                ByteBuffer payload = change.encode();
                if (!consumer.onRecord(change.getKind(), RecordFlag.NONE, entry.getKey(), payload, 0, payload.limit())) {
                    return;
                }
            }
        }
    }
}
