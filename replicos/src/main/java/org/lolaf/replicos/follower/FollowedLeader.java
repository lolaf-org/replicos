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

import lombok.NonNull;
import lombok.Value;

/**
 * The leader a lane's WELCOME named. Its streams' keys hold for its leadership only: another numbers them its own way.
 */
@Value
public class FollowedLeader {

    @NonNull String id;
    long epoch;
    long leadership;

    @Override
    public String toString() {
        return id + " (epoch " + epoch + ", leadership " + Long.toHexString(leadership) + ")";
    }
}
