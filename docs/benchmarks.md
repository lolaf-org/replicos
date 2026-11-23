# Benchmarks

JMH benchmarks in `benchmarks/`, of a leader and its follower in one JVM over loopback. The module is not published.

```bash
./mvnw -T1C clean package -DskipTests
cd benchmarks
java -jar target/benchmarks.jar ReplicationRttBenchmark
```

Run them from `benchmarks/`: the crypto providers are loaded from `target/providers`, and the follower's file sinks
write under `target/sink-files`.

`benchmarks/run.sh` runs them all, into `benchmarks/results/`: a JSON result and a log per run, named after the
benchmark, the JDK and the date. Set `BENCHMARK_CPUS`, and `BENCHMARK_PAIR_CPUS` for the leader and follower pair, to
pin the runs to quiet cores.

## What each one measures

| benchmark | question |
|---|---|
| `ReplicationRttBenchmark` | What a `SYNC` mode adds to a write: one record written, then its ack waited for, again and again. |
| `ReplicationLoadBenchmark` | How each mode behaves at a given rate, and where the pair saturates. |
| `FrameCipherBenchmark` | What sealing a record frame costs, and opening it, per cipher, provider and payload size. |
| `ControlFrameCipherBenchmark` | What authenticating the follower's RECEIVED and ACK frames adds to a round trip. |

### The three replication modes

- **`ASYNC`**: the write returns once the record is in the leader's log. Nothing waits for the follower.
- **`SYNC_RECEIVED`**: the write waits until the follower holds the record in memory, before its sink writes it.
- **`SYNC_APPLIED`**: the write waits until the follower's sink wrote it; with `SINK_FILE_SYNC`, forced to the disk.

`ReplicationRttBenchmark` runs the two `SYNC` modes only: `ASYNC` waits for nothing, so a closed loop would measure how
fast the log fills, not a write. `ReplicationLoadBenchmark` compares all three.

## Reading `ReplicationLoadBenchmark`

Its figures are in the log, one line per iteration; JMH's score is only how long the iteration took:

```
Replication load SYNC_APPLIED 4 streams, 50000 records/s asked, 39109 written, 0 acks failed; write latency µs p50 ...
```

- **records/s asked and written**: below the asked rate, the writer cannot keep up and the latency grows with the
  backlog. One thread writes every stream, and in a `SYNC` mode it waits for each ack before the next write, so a
  round trip bounds its rate.
- **write latency**: from each record's due time, not from when it was written, so a write delayed by the one before it
  counts its wait.
- **follower lag**: how far behind the leader the follower is, from a record written to the log to its ack.
- **dropped too slow, snapshots**: a follower a whole log behind is dropped and caught up again by snapshot; in `ASYNC`,
  the sign the rate is more than the follower can take.

## Parameters

Each takes `-p name=VALUE[,VALUE...]`; the values name themselves in the results.

| parameter | values |
|---|---|
| `mode` | `ASYNC`, `SYNC_RECEIVED`, `SYNC_APPLIED` |
| `payloadSize` | `PAYLOAD_200_B`, `PAYLOAD_1_KIB`, `PAYLOAD_16_KIB` |
| `followerSink` | `SINK_MEMORY`, `SINK_FILE` (page cache), `SINK_FILE_SYNC` (forced once per read) |
| `sinkWriterThreads` | `SINK_WRITER_THREADS_0` (the network thread writes), `_1`, `_2` |
| `waiting` | `BACKOFF` (the library's default), `SPIN` (every replication thread busy-spins) |
| `encryption` | `CLEAR`, `AES_GCM_128`, `AES_GCM_256`, `CHACHA20_POLY1305`, `AES_CTR_HMAC_SHA256`, `CHACHA20_HMAC_SHA256` |
| `provider` | `PROVIDER_JDK`, `PROVIDER_CORRETTO`, `PROVIDER_BC`, `PROVIDER_BC_LTS` |
| `streams`, `lanes`, `rate`, `writingTime` | `ReplicationLoadBenchmark` only |

## Caveats

- **Loopback.** Leader and follower share a JVM and a machine: a network between them adds its round trip to every
  `SYNC` figure, and the two compete for the same cores.
- **Where the files go.** `SINK_FILE_SYNC` only measures a disk if the files are on one. The temporary directory is
  often a tmpfs, where a force costs nothing, which is why the sinks write under `target/sink-files`; set
  `-Dreplicos.benchmarks.dir=...` to measure another disk. The pair logs the file system's type when it starts, and
  warns on a tmpfs.
- **Results are the machine's.** Compare figures from the same machine and JDK only, and pin the run to quiet cores
  (`taskset`) when it matters.
