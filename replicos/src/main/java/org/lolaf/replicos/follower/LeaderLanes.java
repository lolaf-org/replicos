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

import org.jspecify.annotations.Nullable;

/**
 * What a lane's connection asks of the follower's other lanes: they all follow one leader, whose lane count they share.
 */
public interface LeaderLanes {

    /**
     * Why the lane must not follow the leader that welcomed it, null when it may: every lane follows lane 0's leader,
     * which a list of leader addresses mixing two nodes would break.
     *
     * @param lane     the lane welcomed, from 0
     * @param leaderId the node id its WELCOME gave
     */
    @Nullable String refuseLeader(int lane, String leaderId);

    /**
     * Told the leader's lane count from its WELCOME, before the stream starts.
     *
     * @param lanes the leader's lane count, at least 1
     */
    void lanesAnnounced(int lanes);
}
