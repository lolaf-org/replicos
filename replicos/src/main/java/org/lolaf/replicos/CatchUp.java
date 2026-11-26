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
import org.lolaf.replicos.wire.CatchUpCheck;

/**
 * How a follower catches up with a stream it holds part of, as after it restarted or a failover: a snapshot of what
 * it misses, from where its copy and the leader's still agree, which this setting checks. Every node of a group needs
 * the same: a follower checking otherwise than its leader gets the whole stream.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CatchUp {

    private static final CatchUp FULL_COPY = new CatchUp(CatchUpCheck.FULL_COPY, 0);

    CatchUpCheck check;
    /**
     * How many of its last records a follower proves its copy with, 0 for none.
     */
    int records;

    /**
     * Copies the whole stream every time: the costliest, but it needs nothing of the application and no copy can keep
     * records the leader never had. The default.
     */
    public static CatchUp fullCopy() {
        return FULL_COPY;
    }

    /**
     * Copies only what follows the follower's last {@code records} records when the leader holds the same at their
     * positions, else the whole stream. Needs no storage beyond the streams': the sinks implement
     * {@link org.lolaf.replicos.spi.ReplicaSink#readLast} and keep what precedes a snapshot's start. Incremental after
     * a follower restart or a failover it followed; a deposed leader holding records the new one never got is copied
     * whole. Blind spot: two histories that parted earlier yet hold byte-identical records at those positions, as
     * records that repeat may; more records narrow it.
     *
     * @throws IllegalArgumentException for fewer than one record
     */
    public static CatchUp lastRecords(int records) {
        if (records < 1) {
            throw new IllegalArgumentException("lastRecords needs at least one record, got " + records);
        }
        return new CatchUp(CatchUpCheck.LAST_RECORDS, records);
    }
}
