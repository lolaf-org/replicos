# Examples

`examples/` holds a key-value store replicated from a leader to a follower, each in its own JVM. The module is not
published.

```bash
./mvnw -T1C clean package -DskipTests
java -XX:-RestrictContended -jar examples/target/examples.jar leader 7000
java -XX:-RestrictContended -jar examples/target/examples.jar follower 7001 localhost:7000    # in a second terminal
```

Each node reads `put <key> <value>`, `remove <key>`, `get <key>`, `list` and `quit` from its standard input; only the
leader writes. A follower started late is caught up by a snapshot of the leader's journal, then applies each change as
it is written. `-XX:-RestrictContended` lets ringos pad its ring buffers; without it, ringos logs why it could not.

## What it shows

| class | role |
|---|---|
| `KeyValueStore` | the application's own storage: the entries and the journal of changes, in memory |
| `KeyValueStream` | the journal as the stream the leader announces, in `SYNC_APPLIED`, and its snapshot source |
| `KeyValueSinks`, `KeyValueSink` | on the follower, apply the leader's stream into this node's store, refusing the leader's own |
| `KeyValueNode` | builds the `ReplicationNode`, leads or follows, and stores each change before appending it |
| `KeyValueMain` | the command line |

A `put` on the leader returns `ACKED` once the follower applied it, `NO_FOLLOWER_IN_SYNC` with none following, and
`TIMED_OUT` when the follower did not answer in time. With no election here, both nodes use epoch 1; an application
takes its epochs from its election.

Both nodes catch up with `CatchUp.lastRecords(16)`: `KeyValueSink` reads its last changes back, and a snapshot keeps
the changes before its start. A node that led and then follows the new leader gets only the changes it missed. The
store being in memory, a restarted node holds nothing and gets the whole journal.
