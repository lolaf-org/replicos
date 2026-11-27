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
package org.lolaf.replicos.benchmarks.impl;

import lombok.Getter;
import org.jspecify.annotations.Nullable;
import org.lolaf.replicos.AckWait;
import org.lolaf.replicos.StreamWriter;
import org.lolaf.replicos.spi.LeaderStream;
import org.lolaf.replicos.spi.RecordFlag;
import org.lolaf.replicos.spi.SnapshotSource;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;

/**
 * One stream of fixed-size records, of which it keeps the last {@link #HISTORY}: a follower catching up by snapshot
 * gets those only, as from a store that truncates its history. Written by one thread at a time; allocates nothing.
 */
public final class BenchmarkStream implements LeaderStream {

    public static final byte CONTENT_TYPE = 1;
    private static final int HISTORY = 1024;
    private static final int HISTORY_MASK = HISTORY - 1;
    private static final Duration ACK_TIMEOUT = Duration.ofSeconds(5);

    @Getter
    private final int key;
    @Getter
    private final String streamName;
    @Getter
    private final StreamReplicationMode streamReplicationMode;
    private final int payloadBytes;
    private final ByteBuffer[] history;
    private final @Nullable AckWait ackWait;
    private volatile long lastPosition;
    private StreamWriter writer;

    BenchmarkStream(int key, StreamReplicationMode streamReplicationMode, int payloadBytes) {
        this.key = key;
        this.streamName = "stream-" + key;
        this.streamReplicationMode = streamReplicationMode;
        this.payloadBytes = payloadBytes;
        this.history = new ByteBuffer[HISTORY];
        for (int i = 0; i < HISTORY; i++) {
            history[i] = ByteBuffer.allocateDirect(payloadBytes);
        }
        this.ackWait = streamReplicationMode.isSync() ? AckWait.of(streamReplicationMode, ACK_TIMEOUT) : null;
        this.lastPosition = 0;
    }

    void writeThrough(StreamWriter streamWriter) {
        this.writer = streamWriter;
    }

    /**
     * Stores the record, then appends it, in the order the library asks.
     *
     * @param payload {@code payloadBytes} from position 0, its position left at 0
     * @return the record's log offset, for an ack wait
     */
    public long write(ByteBuffer payload) {
        long position = lastPosition + 1;
        ByteBuffer stored = history[(int) (position & HISTORY_MASK)];
        stored.clear();
        stored.put(payload);
        payload.rewind();
        lastPosition = position;
        return writer.append(CONTENT_TYPE, RecordFlag.NONE, position, payload, 0, payloadBytes);
    }

    /**
     * Only for a {@code SYNC} stream.
     */
    public StreamWriter.Ack awaitAck(long offset) {
        return writer.awaitAck(offset, Objects.requireNonNull(ackWait));
    }

    @Override
    public String getStorageIdentity() {
        return "leader-memory";
    }

    @Override
    public SnapshotSource newSnapshotSource() {
        return new HistorySnapshot();
    }

    /**
     * Its state is the next position. A record the writer overwrites while it is read goes out with the newer bytes:
     * the benchmarks never read what they replicate.
     */
    private final class HistorySnapshot implements SnapshotSource {

        @Override
        public boolean isReadable() {
            return true;
        }

        @Override
        public long captureState(ByteBuffer state) {
            long last = lastPosition;
            state.putLong(last + 1);
            return last;
        }

        @Override
        public void read(long fromPosition, long toPosition, RecordConsumer consumer) {
            long oldestHeld = Math.max(1, lastPosition - HISTORY + 1);
            for (long position = Math.max(fromPosition, oldestHeld); position <= toPosition; position++) {
                if (!consumer.onRecord(CONTENT_TYPE, RecordFlag.NONE, position,
                        history[(int) (position & HISTORY_MASK)], 0, payloadBytes)) {
                    return;
                }
            }
        }
    }
}
