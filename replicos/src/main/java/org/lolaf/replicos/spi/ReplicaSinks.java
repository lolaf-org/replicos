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
package org.lolaf.replicos.spi;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import org.lolaf.replicos.spi.StreamAnswer;

/**
 * Where a follower applies the streams its leader announces, from its own configuration. Called on a lane's network
 * thread, a stream being announced rarely.
 */
public interface ReplicaSinks {

    /**
     * Sent in HELLO; a leader replicating another application is refused.
     */
    int getApplicationId();

    /**
     * Opens the sink the stream is applied into, or refuses it. Never called for a stream the leader does not replicate.
     * How the stream is replicated is the leader's alone to decide, so the sink is the same whatever its mode.
     *
     * @param streamName            the leader's name for the stream, which this node finds its copy by
     * @param leaderStorageIdentity where the leader stores the stream, null when it does not know: a stream stored in
     *                              the same place here must be refused, as replicating into it would rewrite the
     *                              leader's data
     */
    Binding open(String streamName, @Nullable String leaderStorageIdentity);

    /**
     * Whether this node's configuration knows the stream: one refused as {@link StreamAnswer#UNKNOWN_STREAM} is bound
     * again once it does.
     *
     * @param streamName the leader's name for the stream
     */
    boolean isConfigured(String streamName);

    /**
     * A stream replicated into a sink, or refused.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    final class Binding {

        private final @Nullable ReplicaSink sink;
        private final StreamAnswer answer;

        /**
         * @param sink applies the stream from its first snapshot on
         */
        public static Binding replicated(ReplicaSink sink) {
            return new Binding(sink, StreamAnswer.SNAPSHOT_NEEDED);
        }

        /**
         * @param refusal one of the {@link StreamAnswer#isRefusal() refusals}
         */
        public static Binding refused(StreamAnswer refusal) {
            if (!refusal.isRefusal()) {
                throw new IllegalArgumentException(refusal + " is not a refusal");
            }
            return new Binding(null, refusal);
        }
    }
}
