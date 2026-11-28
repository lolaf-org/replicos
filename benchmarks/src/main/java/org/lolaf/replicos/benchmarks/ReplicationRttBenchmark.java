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

import org.lolaf.replicos.StreamWriter;
import org.lolaf.replicos.benchmarks.impl.BenchmarkStream;
import org.lolaf.replicos.benchmarks.impl.CipherParams;
import org.lolaf.replicos.benchmarks.impl.ReplicationPair;
import org.lolaf.replicos.benchmarks.impl.ReplicationParams;
import org.lolaf.replicos.spi.StreamReplicationMode;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

/**
 * One write waiting for its acknowledgement, again and again: what a {@code SYNC} mode adds to a write.
 * {@code SYNC_RECEIVED} waits for the follower to hold the record, {@code SYNC_APPLIED} for its sink to have written it,
 * forced to the disk with {@code SINK_FILE_SYNC}. {@code ASYNC} waits for nothing, so a closed loop would only measure
 * how fast the log fills: {@link ReplicationLoadBenchmark} compares the three modes at a given rate.
 */
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 5)
@Fork(value = 1, jvmArgsPrepend = {
        "--enable-native-access=ALL-UNNAMED",
        "-XX:-RestrictContended", "-XX:ContendedPaddingWidth=64", "-XX:AutoBoxCacheMax=65536",
        "--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "-Xmx2g", "-Xms2g"})
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class ReplicationRttBenchmark {


    @Param({"SYNC_RECEIVED", "SYNC_APPLIED"})
    StreamReplicationMode mode;
    @Param({"PAYLOAD_200_B", "PAYLOAD_1_KIB"})
    ReplicationParams.PayloadSize payloadSize;
    @Param({"SINK_MEMORY", "SINK_FILE", "SINK_FILE_SYNC"})
    ReplicationParams.FollowerSink followerSink;
    @Param({"SINK_WRITER_THREADS_0"})
    ReplicationParams.SinkWriterThreads sinkWriterThreads;
    @Param({"BACKOFF"})
    ReplicationParams.Waiting waiting;
    @Param({"CLEAR"})
    CipherParams.Encryption encryption;
    @Param({"PROVIDER_JDK"})
    CipherParams.CryptoProvider provider;

    private ReplicationPair pair;
    private BenchmarkStream stream;
    private ByteBuffer payload;

    @Setup(Level.Trial)
    public void setup() throws GeneralSecurityException {
        payload = ByteBuffer.allocateDirect(payloadSize.getBytes());
        pair = ReplicationPair.builder()
                .streamCount(1)
                .mode(mode)
                .payloadBytes(payloadSize.getBytes())
                .followerSink(followerSink)
                .lanes(1)
                .sinkWriterThreads(sinkWriterThreads.getCount())
                .waiting(waiting)
                .encryption(encryption.settings(provider.install()))
                .start();
        stream = pair.getStreams().stream(0);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pair.close();
    }

    @Benchmark
    public long write() {
        long offset = stream.write(payload);
        StreamWriter.Ack ack = stream.awaitAck(offset);
        if (ack != StreamWriter.Ack.ACKED) {
            throw new IllegalStateException("The follower did not acknowledge the write: " + ack);
        }
        return offset;
    }
}
