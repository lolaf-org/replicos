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
import org.lolaf.replicos.spi.LeaderStream;
import org.lolaf.replicos.spi.LeaderStreams;
import org.lolaf.replicos.spi.StreamReplicationMode;

/**
 * The application the benchmarks replicate, on the leader: streams of records of one size, held in memory.
 */
public final class BenchmarkStreams implements LeaderStreams {

    /**
     * {@code 'BNCH'}.
     */
    public static final int APPLICATION_ID = 0x424e4348;

    private final BenchmarkStream[] streams;

    public BenchmarkStreams(int count, StreamReplicationMode mode, int payloadBytes) {
        this.streams = new BenchmarkStream[count];
        for (int key = 0; key < count; key++) {
            streams[key] = new BenchmarkStream(key, mode, payloadBytes);
        }
    }

    /**
     * Before the first write, once the node is built: the node needs the streams first.
     */
    public void writeThrough(ReplicationNode node) {
        for (BenchmarkStream stream : streams) {
            stream.writeThrough(node.writer(stream.getKey()));
        }
    }

    public BenchmarkStream stream(int key) {
        return streams[key];
    }

    @Override
    public int getApplicationId() {
        return APPLICATION_ID;
    }

    @Override
    public int count() {
        return streams.length;
    }

    @Override
    public LeaderStream get(int key) {
        return streams[key];
    }
}
