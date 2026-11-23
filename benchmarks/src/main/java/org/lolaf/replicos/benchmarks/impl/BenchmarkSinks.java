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

import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.lolaf.replicos.spi.ReplicaSink;
import org.lolaf.replicos.spi.ReplicaSinks;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.spi.StreamReplicationMode;
import org.lolaf.ringos.Deadline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

/**
 * The follower side of {@link BenchmarkStreams}: every stream the leader announces in the mode this node expects is
 * applied into a sink of the kind the benchmark asks for. File sinks write under {@code replicos.benchmarks.dir},
 * {@code target/sink-files} by default rather than the temporary directory, often a tmpfs where a force costs nothing.
 */
@Slf4j
public final class BenchmarkSinks implements ReplicaSinks, AutoCloseable {

    private final ReplicationParams.FollowerSink kind;
    private final StreamReplicationMode mode;
    private final int payloadBytes;
    private final Path directory;
    private final List<ReplicaSink> opened;

    public BenchmarkSinks(ReplicationParams.FollowerSink kind, StreamReplicationMode mode, int payloadBytes)
            throws IOException {
        this.kind = kind;
        this.mode = mode;
        this.payloadBytes = payloadBytes;
        Path parent = Paths.get(System.getProperty("replicos.benchmarks.dir", "target/sink-files"));
        Files.createDirectories(parent);
        this.directory = Files.createTempDirectory(parent, "sinks");
        this.opened = new CopyOnWriteArrayList<>();
    }

    /**
     * Where the file sinks write, and its file system's type.
     */
    public String describeStorage() throws IOException {
        if (kind == ReplicationParams.FollowerSink.SINK_MEMORY) {
            return "memory";
        }
        String type = Files.getFileStore(directory).type();
        if (kind == ReplicationParams.FollowerSink.SINK_FILE_SYNC && "tmpfs".equals(type)) {
            log.warn("The sink files are on a tmpfs, where forcing them costs nothing: set -Dreplicos.benchmarks.dir");
        }
        return directory.toAbsolutePath() + " (" + type + ")";
    }

    @Override
    public int getApplicationId() {
        return BenchmarkStreams.APPLICATION_ID;
    }

    @Override
    public Binding open(String streamName, StreamReplicationMode leaderMode, @Nullable String leaderStorageIdentity) {
        if (leaderMode != mode) {
            return Binding.refused(StreamAnswer.MODE_MISMATCH);
        }
        ReplicaSink sink = kind == ReplicationParams.FollowerSink.SINK_MEMORY
                ? new MemorySink(payloadBytes)
                : FileSink.open(directory.resolve(streamName), payloadBytes,
                kind == ReplicationParams.FollowerSink.SINK_FILE_SYNC);
        opened.add(sink);
        return Binding.replicated(sink);
    }

    @Override
    public StreamReplicationMode modeHere(String streamName) {
        return mode;
    }

    @Override
    public void close() throws IOException {
        for (ReplicaSink sink : opened) {
            sink.stop(Deadline.immediate());
        }
        try (Stream<Path> files = Files.walk(directory)) {
            files.sorted(Comparator.reverseOrder()).forEach(BenchmarkSinks::delete);
        }
    }

    private static void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
