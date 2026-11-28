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
package org.lolaf.replicos.benchmarks;

import org.lolaf.replicos.benchmarks.impl.ReplicationLoad;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/**
 * Writes at a fixed rate over several streams, an open loop, in each replication mode: where a pair saturates, and
 * what each mode costs a write below that. The figures are in the logs, not in JMH's score, which is only how long
 * each iteration took: write latency percentiles from each record's due time, so a write delayed by the one before it
 * counts its wait, and the follower's lag behind the leader.
 */
@Warmup(iterations = 1, batchSize = 1)
@Measurement(iterations = 3, batchSize = 1)
@Fork(value = 1, jvmArgsPrepend = {
        "--enable-native-access=ALL-UNNAMED",
        "-XX:-RestrictContended", "-XX:ContendedPaddingWidth=64", "-XX:AutoBoxCacheMax=65536",
        "--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "-Xmx2g", "-Xms2g"})
public class ReplicationLoadBenchmark {

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public long openLoop(ReplicationLoad load) {
        return load.run();
    }
}
