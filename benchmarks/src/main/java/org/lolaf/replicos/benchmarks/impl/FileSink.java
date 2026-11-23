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

import org.lolaf.replicos.spi.ReplicaSink;
import org.lolaf.ringos.Deadline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Appends each record to a file, going back to its start past {@link #FILE_BYTES} so a long run does not fill the
 * disk. With {@code sync}, the writes of a read are forced to the disk before the follower acknowledges them as
 * applied, one force per read rather than per record.
 */
final class FileSink implements ReplicaSink {

    private static final long FILE_BYTES = 256L << 20;

    private final FileChannel channel;
    private final ByteBuffer copy;
    private final boolean sync;
    private long filePosition;
    private boolean unforced;

    private FileSink(FileChannel channel, int payloadBytes, boolean sync) {
        this.channel = channel;
        this.copy = ByteBuffer.allocateDirect(payloadBytes);
        this.sync = sync;
        this.filePosition = 0;
        this.unforced = false;
    }

    static FileSink open(Path file, int payloadBytes, boolean sync) {
        try {
            return new FileSink(FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING), payloadBytes, sync);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void apply(byte contentType, long position, ByteBuffer buffer, int payloadIndex, int payloadLength) {
        Payloads.copy(buffer, payloadIndex, payloadLength, copy);
        copy.limit(payloadLength).position(0);
        try {
            while (copy.hasRemaining()) {
                filePosition += channel.write(copy, filePosition);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (filePosition + copy.capacity() > FILE_BYTES) {
            filePosition = 0;
        }
        copy.clear();
        unforced = true;
    }

    @Override
    public void snapshotBegin(ByteBuffer buffer, int stateIndex, int stateLength) {
        filePosition = 0;
    }

    @Override
    public void snapshotRecord(byte contentType, long position, ByteBuffer buffer, int payloadIndex, int payloadLength) {
        apply(contentType, position, buffer, payloadIndex, payloadLength);
    }

    @Override
    public void snapshotEnd(ByteBuffer buffer, int stateIndex, int stateLength) {
        writePending();
    }

    @Override
    public void writePending() {
        if (!sync || !unforced) {
            return;
        }
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        unforced = false;
    }

    @Override
    public void discardPending() {
        // every record is written as it is applied
    }

    @Override
    public long getPendingWrites() {
        return 0;
    }

    @Override
    public void stop(Deadline deadline) {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
