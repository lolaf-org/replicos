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

import lombok.Builder;
import lombok.Value;

/**
 * The last record a follower wrote on one lane. Its offset is in the log of the leadership that sent it, so two
 * followers' positions compare only at the same epoch and leadership.
 */
@Value
@Builder
public class LanePosition {

    int lane;
    /**
     * 0 before the follower wrote any record.
     */
    long epoch;
    /**
     * The random id the leader drew when it started leading; 0 before the follower wrote any record.
     */
    long leadership;
    /**
     * The record's offset in the lane's log, -1 for none since the lane last connected without resuming.
     */
    long offset;
}
