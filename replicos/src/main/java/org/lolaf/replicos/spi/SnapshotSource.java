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
package org.lolaf.replicos.spi;

import java.nio.ByteBuffer;

/**
 * A stream's state and records, as a snapshot copies them while the stream keeps being written. The snapshot copies
 * in rounds, each up to the last position the state covered when the round started, so it relies on two things: a
 * record is readable here before it is appended to the log, and positions grow between two resets. One sender thread
 * at a time.
 */
public interface SnapshotSource {

    /**
     * False while the stream cannot be read, as before its storage started or once it stopped: the snapshot is retried
     * shortly, as is one whose read failed once its source stopped.
     */
    boolean isReadable();

    /**
     * Writes the stream's current state into {@code state}, cleared, up to its position, at most
     * {@link org.lolaf.replicos.wire.WireFormat#MAX_STATE_BYTES}: the follower resumes the stream from it.
     *
     * @param state handed cleared, its position left at the state's end
     * @return the last position the state covers, which the round copies up to
     */
    long captureState(ByteBuffer state);

    /**
     * Hands {@code consumer} the records from {@code fromPosition} to {@code toPosition}, both included, in position
     * order, until it returns false. A position the stream holds no record at is skipped.
     *
     * @param fromPosition the first position read
     * @param toPosition   the last position read, which may be past the stream's end
     * @param consumer     copies or digests each record, during the call only
     */
    void read(long fromPosition, long toPosition, RecordConsumer consumer);

    @FunctionalInterface
    interface RecordConsumer {

        /**
         * @param contentType   the application's, as it appended the record
         * @param flag          the record's, as it appended it; {@link RecordFlag#RESET} is ignored here
         * @param position      the record's place in the stream
         * @param buffer        holds the payload at {@code payloadIndex}, read during the call only and left untouched
         * @param payloadIndex  where the payload starts in {@code buffer}
         * @param payloadLength 0 for a record with no payload
         * @return false to stop before the next record
         */
        boolean onRecord(byte contentType, RecordFlag flag, long position, ByteBuffer buffer, int payloadIndex,
                         int payloadLength);
    }
}
