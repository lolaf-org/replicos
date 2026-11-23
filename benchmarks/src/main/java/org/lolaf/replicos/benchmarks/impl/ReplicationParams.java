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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;

/**
 * The replication benchmarks' parameter values, named so that a result's label reads on its own: JMH records a
 * parameter's value without its name, and {@code 4 1 MEMORY} does not say which parameter is which.
 */
@UtilityClass
public class ReplicationParams {

    @Getter
    @RequiredArgsConstructor
    public enum PayloadSize {
        PAYLOAD_200_B(200),
        PAYLOAD_1_KIB(1024),
        PAYLOAD_16_KIB(16 * 1024);

        private final int bytes;
    }

    /**
     * Where the follower applies what it receives. {@code SINK_FILE} writes to the page cache only; {@code
     * SINK_FILE_SYNC} also forces each read's writes to the disk before acknowledging them as applied.
     */
    public enum FollowerSink {
        SINK_MEMORY,
        SINK_FILE,
        SINK_FILE_SYNC
    }

    /**
     * How the replication's threads wait: {@code BACKOFF}, the library's default, or {@code SPIN}, where the leader's
     * senders, the writer's ack waits, the follower's sink writers and both network threads busy-spin.
     */
    public enum Waiting {
        BACKOFF,
        SPIN
    }

    /**
     * The follower's threads writing into its sinks; with none, each lane's network thread writes, and a
     * {@code SYNC_RECEIVED} record waits for the write before it.
     */
    @Getter
    @RequiredArgsConstructor
    public enum SinkWriterThreads {
        SINK_WRITER_THREADS_0(0),
        SINK_WRITER_THREADS_1(1),
        SINK_WRITER_THREADS_2(2);

        private final int count;
    }

    @Getter
    @RequiredArgsConstructor
    public enum Lanes {
        LANES_1(1),
        LANES_2(2),
        LANES_4(4);

        private final int count;
    }

    @Getter
    @RequiredArgsConstructor
    public enum Streams {
        STREAMS_1(1),
        STREAMS_4(4),
        STREAMS_16(16);

        private final int count;
    }

    @Getter
    @RequiredArgsConstructor
    public enum Rate {
        RATE_10000_PER_S(10_000),
        RATE_50000_PER_S(50_000),
        RATE_100000_PER_S(100_000),
        RATE_200000_PER_S(200_000),
        RATE_400000_PER_S(400_000);

        private final int perSecond;
    }

    @Getter
    @RequiredArgsConstructor
    public enum WritingTime {
        WRITING_5_S(5),
        WRITING_10_S(10),
        WRITING_30_S(30);

        private final int seconds;
    }
}
