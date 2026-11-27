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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.time.Duration;

/**
 * What {@link StreamWriter#awaitAck} waits for. Checked once here, so the wait checks nothing: build it with the
 * stream's settings and reuse it.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AckWait {

    /**
     * The quorum of every sync follower in sync, however many that is.
     */
    public static final int ALL_FOLLOWERS = 0;

    /**
     * {@code SYNC_RECEIVED} or {@code SYNC_APPLIED}.
     */
    StreamReplicationMode mode;
    /**
     * How many sync followers must ack, the first to, or {@link #ALL_FOLLOWERS}.
     */
    int quorum;
    /**
     * 0 to wait as long as enough followers are in sync.
     */
    long timeoutNanos;

    /**
     * As {@link #of(StreamReplicationMode, int, Duration)} with a quorum of {@link #ALL_FOLLOWERS}.
     */
    public static AckWait of(StreamReplicationMode mode, Duration timeout) {
        return of(mode, ALL_FOLLOWERS, timeout);
    }

    /**
     * @param timeout zero to wait as long as enough followers are in sync
     * @throws IllegalArgumentException for a mode that is not {@code SYNC}, a negative quorum or a negative timeout
     */
    public static AckWait of(StreamReplicationMode mode, int quorum, Duration timeout) {
        if (!mode.isSync()) {
            throw new IllegalArgumentException("A wait for " + mode + ", which acknowledges nothing");
        }
        if (quorum < 0) {
            throw new IllegalArgumentException("A quorum of " + quorum + " followers");
        }
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("A timeout of " + timeout);
        }
        return new AckWait(mode, quorum, timeout.toNanos());
    }
}
