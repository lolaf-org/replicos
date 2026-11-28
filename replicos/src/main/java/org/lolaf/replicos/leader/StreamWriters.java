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
package org.lolaf.replicos.leader;

import lombok.RequiredArgsConstructor;
import org.lolaf.replicos.LeaderSettings;
import org.lolaf.replicos.StreamWriter;
import org.lolaf.replicos.log.ReplicationLogs;
import org.lolaf.replicos.spi.LatencyRecorder;
import org.lolaf.replicos.spi.ReplicationClock;

/**
 * Opens the writers of a leading node's streams.
 */
@RequiredArgsConstructor
public final class StreamWriters {

    private final LeaderSettings settings;
    private final ReplicationLogs logs;
    private final Followers followers;
    /**
     * By lane, shared by the writers of its streams.
     */
    private final LatencyRecorder[] logFullWaits;
    private final ReplicationStats stats;
    private final ReplicationClock clock;

    StreamWriter open(int streamKey) {
        return new StreamLogWriter(streamKey, logs.forStream(streamKey), followers, settings.getLogFullTimeout(),
                logFullWaits[logs.laneOf(streamKey)], settings.getAckIdleStrategy().get(), stats, clock);
    }
}
