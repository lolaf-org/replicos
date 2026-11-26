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

import org.lolaf.replicos.spi.RecordFlag;
import org.lolaf.replicos.spi.ReplicaSink;
import org.lolaf.ringos.Deadline;

import java.nio.ByteBuffer;

/**
 * Copies each record into memory, the last over the one before: what a sink costs before any storage.
 */
final class MemorySink implements ReplicaSink {

    private final ByteBuffer copy;

    MemorySink(int payloadBytes) {
        this.copy = ByteBuffer.allocateDirect(payloadBytes);
    }

    @Override
    public void apply(byte contentType, RecordFlag flag, long position, ByteBuffer buffer, int payloadIndex,
                      int payloadLength) {
        Payloads.copy(buffer, payloadIndex, payloadLength, copy);
    }

    @Override
    public void snapshotBegin(long fromPosition, ByteBuffer buffer, int stateIndex, int stateLength) {
        // nothing held to discard
    }

    @Override
    public void snapshotRecord(byte contentType, long position, ByteBuffer buffer, int payloadIndex, int payloadLength) {
        Payloads.copy(buffer, payloadIndex, payloadLength, copy);
    }

    @Override
    public void snapshotEnd(ByteBuffer buffer, int stateIndex, int stateLength) {
        // the state is not kept
    }

    @Override
    public void writePending() {
        // every record is written as it is applied
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
