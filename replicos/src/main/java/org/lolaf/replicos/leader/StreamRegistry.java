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

import lombok.Getter;
import org.jspecify.annotations.Nullable;
import org.lolaf.replicos.RegisteredStream;
import org.lolaf.replicos.StreamSettings;
import org.lolaf.replicos.wire.WireFormat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The streams a node replicates when it leads, keyed from 0 in registration order: the key indexes the senders'
 * arrays and spreads the streams over the lanes in turn.
 */
public final class StreamRegistry {

    /**
     * Sent in HELLO and WELCOME, so a peer of another cluster is refused.
     */
    @Getter
    private final int clusterId;
    private final int lanes;
    private final Followers followers;
    private final ReplicationStats stats;
    private final Map<String, StreamRegistration> byName;
    /**
     * Replaced whole on each registration, so a sender reading it never sees a stream half built.
     */
    private volatile StreamRegistration[] byKey;

    public StreamRegistry(int clusterId, int lanes, Followers followers, ReplicationStats stats) {
        this.clusterId = clusterId;
        this.lanes = lanes;
        this.followers = followers;
        this.stats = stats;
        this.byName = new HashMap<>();
        this.byKey = new StreamRegistration[0];
    }

    private static String requireValidName(String streamName) {
        if (streamName.isEmpty()) {
            throw new IllegalArgumentException("A stream name cannot be empty");
        }
        int bytes = streamName.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > WireFormat.MAX_STRING_BYTES) {
            throw new IllegalArgumentException("A stream name is at most " + WireFormat.MAX_STRING_BYTES
                    + " UTF-8 bytes, got " + bytes);
        }
        return streamName;
    }

    /**
     * @throws IllegalArgumentException for a name empty, longer than the STREAM frame carries, or registered already
     */
    public synchronized StreamRegistration register(StreamSettings settings, StreamWriters writers) {
        String streamName = requireValidName(settings.getName());
        if (byName.containsKey(streamName)) {
            throw new IllegalArgumentException("Stream " + streamName + " is registered already");
        }
        StreamRegistration[] registered = byKey;
        int key = registered.length;
        StreamRegistration added = new StreamRegistration(key, key % lanes, settings, writers, followers, stats);
        StreamRegistration[] grown = Arrays.copyOf(registered, key + 1);
        grown[key] = added;
        byName.put(streamName, added);
        byKey = grown;
        return added;
    }

    public int count() {
        return byKey.length;
    }

    /**
     * @param key from 0 to {@link #count()}, excluded
     */
    public StreamRegistration get(int key) {
        return byKey[key];
    }

    public synchronized @Nullable StreamRegistration find(String streamName) {
        return byName.get(streamName);
    }

    /**
     * In registration order.
     */
    public List<RegisteredStream> list() {
        return List.of(byKey);
    }
}
