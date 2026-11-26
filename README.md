# Replicos

[![build](https://github.com/lolaf-org/replicos/actions/workflows/build.yml/badge.svg)](https://github.com/lolaf-org/replicos/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.txt)

Replicates an application's data from a leader JVM to its followers over TCP, in real time: streams of records,
snapshots to catch a follower up, writes that can wait for the followers' acknowledgements, and optional encryption.
The records are the application's bytes: replicos never reads them.

It replicates; it does not elect. Which node leads, and the epoch numbering each leadership, come from the
application's election, such as [primos](https://github.com/lolaf-org/primos).

- **Java 11 or later**, compiled to Java 11 bytecode.
- **Depends on [betty](https://github.com/lolaf-org/betty)** for its networking,
  **[ringos](https://github.com/lolaf-org/ringos)** for its ring buffers and idle strategies, and on SLF4J.
- **Apache License 2.0.**

```xml
<dependency>
    <groupId>org.lolaf.replicos</groupId>
    <artifactId>replicos</artifactId>
    <version>${replicos.version}</version>
</dependency>
```

## What you get

- **Replication over TCP** of streams of records, from a leader to its followers, each applying them into its own
  storage.
- **A replication mode per stream**: `ASYNC`, or `SYNC_RECEIVED` and `SYNC_APPLIED`, where a write waits until a
  follower received or applied it, with a timeout.
- **Zero allocation on the write path**: appending a record, sending it and acknowledging it allocate nothing, and
  neither does the default cipher.
- **Latency à la carte**: every thread on the replication path waits through an idle strategy you choose. Backing
  off, the default, spares the cores; busy spinning on the leader's senders and ack waits, the follower's sink
  writers and both network threads trades cores for the lowest round trip. Lanes and sink writer threads add
  parallelism where the load needs it.
- **Catching up**: a follower that reconnects to the same leader resumes from the leader's log; one missing history
  gets a snapshot while the stream keeps being written, of the whole stream or, with `CatchUp.byLastRecords()`, only
  of what it misses; one too slow is dropped rather than holding the leader back.
- **Failover support**: your election decides who leads; `handOver` on a clean stop tells the followers they hold
  every write; a follower can be given every address its leader listens on.
- **Optional encryption** with a secret the nodes share: AES-CTR with HMAC-SHA256 by default, AES-GCM, ChaCha20 with
  HMAC or ChaCha20-Poly1305, through any JCE provider, with keys derived per connection.
- **Monitoring**: the state of each follower lane, counters over the node's life, and latency recorders for lag, ack
  round trip, catch up and writes held by a full log.
- **Testing**: `replicos-testkit` feeds a follower the frames a leader would send, to test your own streams and sinks.

## Model

- **A stream** is a sequence of records, each at a position that increases between two resets. The application
  stores its records itself; the library only carries them.
- **A record** has a content type (one byte, the application's), a flag, its position and a payload.
- **A replication mode**, per stream: `ASYNC` waits for nothing, `SYNC_RECEIVED` and `SYNC_APPLIED` let a write wait
  until the followers received or applied it.
- **A snapshot** copies a stream's state and records to a follower that is missing history, while the stream keeps
  being written.

**Start with [docs/how-it-works.md](docs/how-it-works.md)**: a record's path, the log, how a follower catches up
after a disconnect, a restart or a failover, and what each catch-up setting costs, with diagrams. The wire format is
in [docs/protocol.md](docs/protocol.md), a runnable example in [docs/examples.md](docs/examples.md), and the
benchmarks in [docs/benchmarks.md](docs/benchmarks.md).

## What the application implements

The interfaces of `org.lolaf.replicos.spi`:

| interface | role |
|---|---|
| `LeaderStreams`, `LeaderStream` | the streams the leader announces: key, name, mode, where it stores them |
| `SnapshotSource` | reads a stream's state and records back from the application's storage, for a snapshot |
| `ReplicaSinks` | on a follower, opens the sink each announced stream is applied into, or refuses it |
| `ReplicaSink` | applies one stream's records and snapshots into the follower's storage; under `CatchUp.byLastRecords()` it also reads its last records back |

`ReplicationClock`, `ReplicationMetrics` and `LatencyRecorder` are optional: the defaults are the system clock and no
metrics.

## Running a node

```java
ReplicationNode node = ReplicationNode.builder()
        .nodeId("node-a")
        .leader(LeaderSettings.builder()
                .bindAddress(new InetSocketAddress(7000))
                .streams(myLeaderStreams)
                .build())
        .follower(FollowerSettings.builder()
                .sinks(myReplicaSinks)
                .build())
        .build();

// as the election decides; a leader on several networks is given by all its addresses
node.follow(List.of(leaderAddressA, leaderAddressB), () -> knownEpoch, Deadline.of(Duration.ofSeconds(5)));
node.lead(epoch, Deadline.of(Duration.ofSeconds(5)));

// on the leader, once a record is stored
StreamWriter writer = node.writer(streamKey);
long offset = writer.append(contentType, RecordFlag.NONE, position, buffer, index, length);
writer.awaitAck(offset, StreamReplicationMode.SYNC_APPLIED, timeoutNanos);
```

Both nodes give the same `ReplicaSinks.getApplicationId()`; a peer replicating another application is refused.
`handOver` before stopping a leader lets the next one take over each stream where it was left.

A node holds what both roles read (log capacity, encryption, catch-up, network threads' waiting, clock, metrics) and
each role's own settings: `LeaderSettings` (lanes, timeouts, send buffers, snapshot chunks) and `FollowerSettings`
(reconnection, sink writer threads). Everything but the bind address, the streams and the sinks has a default. A node
given one role's settings only refuses to take the other.

## Monitoring

`node.followers()` is the leader's side, `node.leader()` the follower's: their states and their counters over the
node's life, polled from any thread. A `ReplicationMetrics` given to the builder is asked for a `LatencyRecorder` per
duration, which the replication's threads then record into:

| recorder | what it times |
|---|---|
| `followerAckLatency` | a follower's round trip, from a batch sent to its ack |
| `followerLag` | how far behind a follower is, from a record written to its ack |
| `followerCatchUp` | from a follower lane connecting to it holding every stream in sync: how long a failover to it would lose writes |
| `logFullWait` | how long a write waited for a follower a whole log behind, before the log full timeout drops it |
| `receivedWriteLatency` | on a follower, how long the sinks took to write what was acknowledged as received |

Connections closed are counted by the reasons of [docs/protocol.md](docs/protocol.md), and `DISCONNECTED` for a
connection lost without one.

## Rules the application keeps

- **Store, then append.** A record is appended after the application stored it, in position order: a snapshot reads
  it back from there.
- **Flag a reset.** A stream starting over, its positions restarting, is appended with `RecordFlag.RESET`; each
  follower's sink is handed that record flagged, and discards its copy before applying it.
- **One thread per writer.** A `StreamWriter` is used from one thread at a time; it allocates nothing.
- **A sink failure reconnects.** A `ReplicaSink` throwing `WireFormatException` reports the leader's error; any other
  exception is the follower's own failure to apply, after which it reconnects and resumes past the last record
  written.
- **Each node stores its own copy.** `ReplicaSinks.open` refuses a stream the leader stores where this node would
  write it, or whose storage it cannot tell apart: replicating into it would rewrite the leader's data.

## Testing an application against it

`replicos-testkit`, as a test dependency, holds what an application's own tests need: `RecordingHandler` to read the
frames a node sends as lines of text, `AbstractLeaderConnectionHarness` to feed a follower's connection the frames a
leader would send and apply them into the application's sinks, `TestingKeyExchanges` and `AllocatedBytes`.

```xml
<dependency>
    <groupId>org.lolaf.replicos</groupId>
    <artifactId>replicos-testkit</artifactId>
    <version>${replicos.version}</version>
    <scope>test</scope>
</dependency>
```
