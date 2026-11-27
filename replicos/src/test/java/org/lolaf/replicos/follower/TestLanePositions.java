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

import org.junit.jupiter.api.Test;
import org.lolaf.replicos.LanePosition;

import static org.assertj.core.api.Assertions.assertThat;

class TestLanePositions {

    private static LanePosition position(int lane, long epoch, long leadership, long offset) {
        return LanePosition.builder().lane(lane).epoch(epoch).leadership(leadership).offset(offset).build();
    }

    @Test
    void aLaneShowsItsLastRecordOnceWrittenWithTheLeadershipThatSentIt() {
        LanePositions positions = new LanePositions();
        assertThat(positions.toList()).containsExactly(position(0, 0, 0, -1));

        positions.streamStarted(0, false, 3, 41);
        positions.applied(0, 100, false);
        assertThat(positions.toList()).as("applied, not written yet").containsExactly(position(0, 0, 0, -1));

        positions.written(0);
        positions.applied(0, 200, false);
        positions.written(0);
        assertThat(positions.toList()).containsExactly(position(0, 3, 41, 200));
    }

    @Test
    void aLaneNotResumedShowsNoOffsetUntilItWritesAgain() {
        LanePositions positions = new LanePositions();
        positions.streamStarted(0, false, 3, 41);
        positions.applied(0, 100, false);
        positions.written(0);

        positions.streamStarted(0, false, 4, 42);

        assertThat(positions.toList()).containsExactly(position(0, 3, 41, -1));
    }

    @Test
    void aLaneAddedHasWrittenNothing() {
        LanePositions positions = new LanePositions();

        positions.setCount(2);

        assertThat(positions.toList()).containsExactly(position(0, 0, 0, -1), position(1, 0, 0, -1));
    }
}
