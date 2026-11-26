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
import org.lolaf.replicos.spi.WireFormatException;

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
    LAST_RECORDS(1);

    private final int code;

    /**
     * @throws WireFormatException for a code no check has
     */
    public static CatchUpCheck from(int code) {
        switch (code) {
            case 0:
                return FULL_COPY;
            case 1:
                return LAST_RECORDS;
            default:
                throw new WireFormatException("Unknown catch-up check " + code);
        }
    }
}
