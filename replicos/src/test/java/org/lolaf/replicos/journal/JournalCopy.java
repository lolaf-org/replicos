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
package org.lolaf.replicos.journal;

import lombok.Getter;
import lombok.Setter;
import org.jspecify.annotations.Nullable;
import org.lolaf.ringos.Deadline;
import org.lolaf.replicos.spi.ReplicaSink;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * A journal as a follower holds it. Records each call as one line in {@link #getWrites()}, so a test compares them
 * all at once: {@code "3 hello"} for an entry, {@code "snapshot 3 hello"} for one a snapshot copied, the content type
 * after the position when not {@link JournalStream#TEXT}. Keeps what it wrote by position too, as storage would, which
 * {@link #readLast} reads back and a snapshot discards from its start.
 */
public final class JournalCopy implements ReplicaSink {

    @Getter
    private volatile StreamReplicationMode mode;
    @Getter
    private volatile @Nullable String storageIdentity;
    @Getter
    private final List<String> writes;
    /**
     * Entries held until {@link #writePending()} while true, as a sink writing in batches.
     */
    @Setter
    private volatile boolean batching;
    private final List<Write> pending;
    private final NavigableMap<Long, Write> stored;
    /**
     * What {@link #getPendingWrites()} reports.
     */
    @Setter
    private volatile long pendingWrites;
    @Getter
    private volatile boolean stopped;
    /**
     * Each write waits for it, as a slow storage would.
     */
    @Setter
    private volatile CountDownLatch writesHeld;
    /**
     * Whether a held write was interrupted, which would fail the write of a storage that does not swallow it.
     */
    @Getter
    private volatile boolean heldWriteInterrupted;
    /**
     * Fails every write, as a storage gone would.
     */
    @Setter
    private volatile boolean failing;

    JournalCopy() {
        this.mode = StreamReplicationMode.ASYNC;
        this.writes = new CopyOnWriteArrayList<>();
        this.pending = new ArrayList<>();
        this.stored = new ConcurrentSkipListMap<>();
        this.writesHeld = new CountDownLatch(0);
    }

    void configure(StreamReplicationMode mode, @Nullable String storageIdentity) {
        this.mode = mode;
        this.storageIdentity = storageIdentity;
    }

    /**
     * An entry stored before this process started, as a copy left by an earlier run.
     */
    public void hold(long position, String text) {
        stored.put(position, new Write(JournalStream.TEXT, position, text.getBytes(StandardCharsets.US_ASCII), ""));
    }

    private static Write entry(byte contentType, long position, ByteBuffer buffer, int index, int length) {
        byte[] bytes = new byte[length];
        buffer.duplicate().position(index).get(bytes);
        String type = contentType == JournalStream.TEXT ? "" : " type=" + contentType;
        return new Write(contentType, position, bytes, position + type + " " + new String(bytes, StandardCharsets.US_ASCII));
    }

    @Override
    public void apply(byte contentType, long position, ByteBuffer buffer, int payloadIndex, int payloadLength) {
        Write entry = entry(contentType, position, buffer, payloadIndex, payloadLength);
        if (batching) {
            synchronized (pending) {
                pending.add(entry);
            }
        } else {
            write(List.of(entry));
        }
    }

    @Override
    public void readLast(int records, RecordConsumer consumer) {
        List<Write> last = new ArrayList<>(stored.descendingMap().values()).subList(0, Math.min(records, stored.size()));
        for (int i = last.size() - 1; i >= 0; i--) {
            Write entry = last.get(i);
            if (!consumer.onRecord(entry.contentType, entry.position, ByteBuffer.wrap(entry.payload), 0,
                    entry.payload.length)) {
                return;
            }
        }
    }

    private void write(List<Write> entries) {
        if (entries.isEmpty()) {
            return;
        }
        if (failing) {
            throw new IllegalStateException("storage down");
        }
        try {
            writesHeld.await();
        } catch (InterruptedException e) {
            heldWriteInterrupted = true;
            Thread.currentThread().interrupt();
        }
        for (Write entry : entries) {
            stored.put(entry.position, entry);
            writes.add(entry.line);
        }
    }

    @Override
    public void snapshotBegin(long fromPosition, ByteBuffer buffer, int stateIndex, int stateLength) {
        stored.tailMap(fromPosition, true).clear();
        writes.add("begin from=" + fromPosition + " " + state(buffer, stateIndex, stateLength));
    }

    @Override
    public void snapshotRecord(byte contentType, long position, ByteBuffer buffer, int payloadIndex, int payloadLength) {
        Write entry = entry(contentType, position, buffer, payloadIndex, payloadLength);
        stored.put(position, entry);
        writes.add("snapshot " + entry.line);
    }

    @Override
    public void snapshotEnd(ByteBuffer buffer, int stateIndex, int stateLength) {
        writes.add("end " + state(buffer, stateIndex, stateLength));
    }

    private static String state(ByteBuffer buffer, int index, int length) {
        byte[] bytes = new byte[length];
        buffer.duplicate().position(index).get(bytes);
        return "state=" + new String(bytes, StandardCharsets.US_ASCII);
    }

    @Override
    public void writePending() {
        List<Write> entries;
        synchronized (pending) {
            entries = List.copyOf(pending);
            pending.clear();
        }
        write(entries);
    }

    @Override
    public void discardPending() {
        synchronized (pending) {
            pending.clear();
        }
    }

    @Override
    public long getPendingWrites() {
        return pendingWrites;
    }

    @Override
    public void stop(Deadline deadline) {
        stopped = true;
    }

    private static final class Write {

        private final byte contentType;
        private final long position;
        private final byte[] payload;
        /**
         * As {@link #getWrites()} shows it.
         */
        private final String line;

        private Write(byte contentType, long position, byte[] payload, String line) {
            this.contentType = contentType;
            this.position = position;
            this.payload = payload;
            this.line = line;
        }
    }
}
