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
package org.lolaf.replicos.benchmarks.impl;

import org.lolaf.replicos.ReplicationNode;
import org.lolaf.replicos.spi.StreamReplicationMode;

/**
 * The application the benchmarks replicate, on the leader: streams of records of one size, held in memory.
 */
public final class BenchmarkStreams {

    public static final String CLUSTER = "benchmark";

    private final BenchmarkStream[] streams;

    public BenchmarkStreams(int count, StreamReplicationMode mode, int payloadBytes) {
        this.streams = new BenchmarkStream[count];
        for (int index = 0; index < count; index++) {
            streams[index] = new BenchmarkStream(index, mode, payloadBytes);
        }
    }

    /**
     * Registers the streams with the leader, in order, before the first write.
     */
    public void writeThrough(ReplicationNode node) {
        for (BenchmarkStream stream : streams) {
            stream.writeThrough(node.register(stream.settings()).writer());
        }
    }

    public BenchmarkStream stream(int index) {
        return streams[index];
    }
}
