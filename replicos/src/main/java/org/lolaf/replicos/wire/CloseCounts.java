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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The connections closed over a node's life, by the reason either peer gave, or lost without one.
 */
public final class CloseCounts {

    /**
     * The key of the connections lost without a CLOSE.
     */
    public static final String DISCONNECTED = "DISCONNECTED";

    private static final CloseReason[] REASONS = CloseReason.values();

    private final AtomicLongArray byReason;
    private final AtomicLong disconnected;

    public CloseCounts() {
        this.byReason = new AtomicLongArray(REASONS.length);
        this.disconnected = new AtomicLong();
    }

    public void closed(CloseReason reason) {
        byReason.incrementAndGet(reason.ordinal());
    }

    public void disconnected() {
        disconnected.incrementAndGet();
    }

    /**
     * Only the reasons that closed a connection, by name.
     */
    public Map<String, Long> toMap() {
        Map<String, Long> counts = new TreeMap<>();
        for (CloseReason reason : REASONS) {
            long count = byReason.get(reason.ordinal());
            if (count > 0) {
                counts.put(reason.name(), count);
            }
        }
        long lost = disconnected.get();
        if (lost > 0) {
            counts.put(DISCONNECTED, lost);
        }
        return counts;
    }
}
