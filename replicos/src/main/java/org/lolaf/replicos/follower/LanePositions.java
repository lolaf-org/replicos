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
package org.lolaf.replicos.follower;

import org.lolaf.replicos.LanePosition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Where each of the leader's lanes stands on this follower, kept across connections so a reconnection resumes: the
 * last record written, the last applied but not written yet, and whether its connection has announced every stream. A
 * lane's offsets are those of its own log. A lane's position is written by its own thread only; the last written and
 * the announcing are read by any. Allocates nothing but on a lane count change.
 */
final class LanePositions {

    private static final Position NOTHING_APPLIED = new Position();

    private volatile Position[] positions;

    LanePositions() {
        this.positions = new Position[] {new Position()};
    }

    int count() {
        return positions.length;
    }

    /**
     * A lane's position is kept while the count does not change. A lane added is announcing: a WELCOME named it, so
     * the leader has streams for it this follower has not heard of.
     */
    synchronized void setCount(int lanes) {
        Position[] current = positions;
        if (lanes == current.length) {
            return;
        }
        Position[] resized = Arrays.copyOf(current, lanes);
        for (int lane = current.length; lane < lanes; lane++) {
            resized[lane] = new Position();
            resized[lane].announcing = true;
        }
        positions = resized;
    }

    long getLastAppliedEpoch(int lane) {
        return position(lane).lastAppliedEpoch;
    }

    long getLastAppliedLeadership(int lane) {
        return position(lane).lastAppliedLeadership;
    }

    long getLastAppliedOffset(int lane) {
        return position(lane).lastAppliedOffset;
    }

    /**
     * Every lane's last record written, its epoch, leadership and offset read together. Any thread.
     */
    List<LanePosition> toList() {
        Position[] current = positions;
        List<LanePosition> lanes = new ArrayList<>(current.length);
        for (int lane = 0; lane < current.length; lane++) {
            lanes.add(current[lane].read(lane));
        }
        return lanes;
    }

    /**
     * See {@link FollowerStreams#takeReceivedToAck(int)}.
     */
    long takeReceivedToAck(int lane) {
        Position position = position(lane);
        if (!position.receivedToAck) {
            return -1;
        }
        position.receivedToAck = false;
        return position.pendingOffset;
    }

    /**
     * @param resumed    false leaves nothing to resume from
     * @param leadership the WELCOME's, which the records applied from now on come from, with {@code epoch}
     */
    void streamStarted(int lane, boolean resumed, long epoch, long leadership) {
        Position position = positions[lane];
        if (!resumed) {
            position.lastAppliedOffset = -1;
        }
        position.streamEpoch = epoch;
        position.streamLeadership = leadership;
        position.pendingEpoch = position.lastAppliedEpoch;
        position.pendingLeadership = position.lastAppliedLeadership;
        position.pendingOffset = position.lastAppliedOffset;
        position.receivedToAck = false;
        position.announcing = true;
    }

    void streamsAnnounced(int lane) {
        positions[lane].announcing = false;
    }

    /**
     * Whether a lane's connection has not announced all its streams yet. Allocates nothing.
     */
    boolean isAnnouncing(int lane) {
        return position(lane).announcing;
    }

    boolean isAnyAnnouncing() {
        for (Position position : positions) {
            if (position.announcing) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param acksOnReceipt the record's stream is acknowledged as received
     */
    void applied(int lane, long offset, boolean acksOnReceipt) {
        Position position = positions[lane];
        position.pendingEpoch = position.streamEpoch;
        position.pendingLeadership = position.streamLeadership;
        position.pendingOffset = offset;
        position.receivedToAck |= acksOnReceipt;
    }

    /**
     * What was applied is now written.
     */
    void written(int lane) {
        Position position = positions[lane];
        if (position.pendingLeadership == position.lastAppliedLeadership
                && position.pendingEpoch == position.lastAppliedEpoch) {
            position.lastAppliedOffset = position.pendingOffset;
            return;
        }
        position.changes++;
        position.lastAppliedEpoch = position.pendingEpoch;
        position.lastAppliedLeadership = position.pendingLeadership;
        position.lastAppliedOffset = position.pendingOffset;
        position.changes++;
    }

    private Position position(int lane) {
        Position[] current = positions;
        return lane < current.length ? current[lane] : NOTHING_APPLIED;
    }

    private static final class Position {
        /**
         * Odd while the leadership of the last record written changes, so a reader on another thread sees no epoch
         * and leadership with another's offset.
         */
        private volatile int changes;
        private volatile long lastAppliedEpoch;
        private volatile long lastAppliedLeadership;
        private volatile long lastAppliedOffset;
        private volatile boolean announcing;
        private long streamEpoch;
        private long streamLeadership;
        private long pendingEpoch;
        private long pendingLeadership;
        private long pendingOffset;
        private boolean receivedToAck;

        private Position() {
            this.lastAppliedOffset = -1;
            this.pendingOffset = -1;
        }

        private LanePosition read(int lane) {
            while (true) {
                int before = changes;
                long epoch = lastAppliedEpoch;
                long leadership = lastAppliedLeadership;
                long offset = lastAppliedOffset;
                if ((before & 1) == 0 && changes == before) {
                    return LanePosition.builder().lane(lane).epoch(epoch).leadership(leadership).offset(offset).build();
                }
                Thread.onSpinWait();
            }
        }
    }
}
