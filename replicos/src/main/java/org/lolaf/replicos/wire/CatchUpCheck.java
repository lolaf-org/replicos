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
package org.lolaf.replicos.wire;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * How a follower proves, in a {@link FrameType#STREAM_ANSWER}, which part of a stream it holds that the leader's copy
 * agrees with, so that a snapshot copies only the rest.
 */
@Getter
@RequiredArgsConstructor
public enum CatchUpCheck {

    /**
     * No proof: the snapshot copies the whole stream.
     */
    FULL_COPY(0),
    /**
     * A {@link RecordDigest} of the follower's last records: the snapshot copies what follows them when the leader's
     * records at their positions digest the same.
     */
    LAST_RECORDS(1),
    /**
     * A check this version does not know, from a newer peer: never sent, it gets the whole stream. Code 2 is reserved
     * for a check by the history of the leaderships that wrote each stream.
     */
    UNKNOWN(-1);

    private final int code;

    /**
     * @return {@link #UNKNOWN} for a code no check of this version has
     */
    public static CatchUpCheck from(int code) {
        switch (code) {
            case 0:
                return FULL_COPY;
            case 1:
                return LAST_RECORDS;
            default:
                return UNKNOWN;
        }
    }
}
