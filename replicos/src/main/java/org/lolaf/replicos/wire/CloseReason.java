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

import java.util.Arrays;

/**
 * Why a peer closes the replication stream, sent in a {@link FrameType#CLOSE} before the connection drops.
 */
@Getter
@RequiredArgsConstructor
public enum CloseReason {

    SHUTDOWN(0),
    VERSION_MISMATCH(1),
    /**
     * The other side's epoch is older than this side's: it leads, or follows, a leadership that is over.
     */
    STALE_EPOCH(2),
    PROTOCOL_ERROR(3),
    /**
     * The leader could not read a stream's snapshot source; the follower reconnects to try again.
     */
    SNAPSHOT_FAILED(4),
    /**
     * The follower could not apply what it received, a sink failing to write: it reconnects to resume after the last
     * record it applied.
     */
    APPLY_FAILED(5),
    /**
     * The follower owed an acknowledgement and stayed silent for the stall timeout: it reconnects and is caught up.
     */
    STALLED(6),
    /**
     * The follower read the log so slowly that a write waited the log full timeout for it: it reconnects and is
     * caught up by snapshot.
     */
    TOO_SLOW(7),
    /**
     * The follower was dropped as too slow and is held off before it may resync: it tries again at its next reconnect.
     */
    RETRY_LATER(8),
    /**
     * The follower opened a lane past the count the leader announced in its WELCOME.
     */
    NO_SUCH_LANE(9),
    /**
     * The peers encrypt with different secrets or ciphers: the follower's KEY_CONFIRMATION does not verify.
     */
    KEY_MISMATCH(10),
    /**
     * One peer requires encryption and the other has it disabled.
     */
    ENCRYPTION_REQUIRED(11),
    /**
     * The peers belong to different clusters.
     */
    CLUSTER_MISMATCH(12),
    /**
     * A follower's lane reached another leader than its lane 0 follows.
     */
    LEADER_MISMATCH(13),
    /**
     * A follower connected a lane another connection of the same node id holds: two nodes were given one id.
     */
    DUPLICATE_NODE_ID(14),
    /**
     * Any other code, from a newer peer; kept rather than refused so the close is still reported.
     */
    UNKNOWN(0xFFFF);

    private static final CloseReason[] BY_CODE = byCode();

    private final int code;

    private static CloseReason[] byCode() {
        CloseReason[] known = Arrays.stream(values()).filter(reason -> reason != UNKNOWN).toArray(CloseReason[]::new);
        CloseReason[] byCode = new CloseReason[Arrays.stream(known).mapToInt(reason -> reason.code).max().orElse(0) + 1];
        for (CloseReason reason : known) {
            byCode[reason.code] = reason;
        }
        return byCode;
    }

    /**
     * @return {@link #UNKNOWN} for a code no reason has
     */
    public static CloseReason from(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : UNKNOWN;
    }

    /**
     * The peers disagree on encryption, or two share a node id: logged as an error at both ends, since only an
     * operator can fix it.
     */
    public boolean isConfigurationError() {
        return this == KEY_MISMATCH || this == ENCRYPTION_REQUIRED || this == DUPLICATE_NODE_ID;
    }
}
