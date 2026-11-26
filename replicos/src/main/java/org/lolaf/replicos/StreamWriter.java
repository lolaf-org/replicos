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
import org.lolaf.replicos.spi.RecordFlag;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.nio.ByteBuffer;

/**
 * Appends one stream's records to the leader's log and waits for its followers' acks, from one thread at a time.
 * Allocates nothing.
 */
public interface StreamWriter extends AutoCloseable {

    /**
     * What a wait for the followers' acks ended with.
     */
    enum Ack {
        ACKED,
        TIMED_OUT,
        NO_FOLLOWER_IN_SYNC
    }

    /**
     * Records must be appended in their position order, the application having stored each one first: a follower
     * catching up by snapshot reads them from there. Waits, up to the log full timeout, for a follower a whole log
     * behind, then drops it.
     *
     * @param contentType   the application's, telling a follower's sink what the payload holds
     * @param flag          {@link RecordFlag#RESET} to start the stream over at this record,
     *                      {@link RecordFlag#PLAINTEXT} to send its payload unencrypted, else {@link RecordFlag#NONE}
     * @param position      the record's place in its stream, increasing between two resets: a snapshot reads the
     *                      stream back by it
     * @param buffer        holds the payload at {@code payloadIndex}, copied into the log and left untouched; null for
     *                      no payload
     * @param payloadIndex  where the payload starts in {@code buffer}
     * @param payloadLength 0 for no payload; longer than a frame, it is sent in fragments
     * @return the offset in the log of the record, or of a long payload's last fragment, for {@link #awaitAck}
     * @throws IllegalArgumentException for a payload longer than {@link #getMaxPayloadLength()}, left out of the log
     */
    long append(byte contentType, RecordFlag flag, long position, @Nullable ByteBuffer buffer, int payloadIndex,
                int payloadLength);

    /**
     * The longest payload {@link #append} takes, half the log's capacity; one longer than a frame goes in fragments.
     */
    int getMaxPayloadLength();

    /**
     * Waits until every follower holding the stream in sync received or applied the record at {@code offset}, as
     * {@code mode} asks. A follower a wait timed out on is not waited for again until it applies what it missed, so a
     * hung follower costs one timeout, not one per record.
     *
     * @param offset       as {@link #append} returned it
     * @param mode         {@code SYNC_RECEIVED} or {@code SYNC_APPLIED}
     * @param timeoutNanos 0 to wait as long as a follower is in sync
     */
    Ack awaitAck(long offset, StreamReplicationMode mode, long timeoutNanos);

    /**
     * Stops waking this writer's ack waits, which the acks wake from the first wait on; a later wait is woken again.
     */
    @Override
    void close();
}
