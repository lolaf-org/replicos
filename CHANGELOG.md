# Changelog

All notable changes to this project are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Each released version needs its own `## [x.y.z] - YYYY-MM-DD` heading here **before** the release is
cut: the release workflow refuses to run without one, and the GitHub Release for the tag is created
with that section as its body. Write it in the commit that precedes the release, together with the
matching `[x.y.z]:` link definition at the foot of the file.

## [Unreleased]

## [0.9.0] - 2026-11-23

The first release, extracted from the high availability of [staffix](https://github.com/lolaf-org/staffix).

### Added

- **Streams of records replicated from a leader to its followers over TCP.** Your application stores its records and
  registers its streams with the node, which announces them; replicos carries the bytes, never reads them, and has
  each follower apply them into its own storage. `docs/how-it-works.md` explains how, with diagrams; the wire protocol
  is in `docs/protocol.md`.
- **Three replication modes, per stream.** `ASYNC` waits for nothing; `SYNC_RECEIVED` and `SYNC_APPLIED` let a write
  wait until a follower received or applied it. The wait has a timeout, and tells a timed-out write from one with no
  follower in sync.
- **Snapshots catch up a follower missing history** while the stream keeps being written. `resync` sends a stream
  again from scratch.
- **A stream can start over.** A record appended with `RecordFlag.RESET` restarts the stream's positions, and each
  follower's sink is handed it with that flag, so it discards its copy first.
- **A follower can be copied only what it misses.** With `CatchUp.byLastRecords()` on every node, a follower that
  restarted, was dropped, or followed the previous leader keeps its copy and receives only the records after it,
  once its last 16 records match the leader's; one holding records the leader never had gets the whole stream. Its
  sink reads its last records back with `readLast`. The default, `CatchUp.fullCopy()`, copies the whole stream every
  time.
- **Followers recover on their own.** A follower that reconnects to the same leader resumes from where it was, as
  long as the leader's log still holds it; otherwise it is caught up by snapshot. A follower too slow to keep up is
  dropped, so it cannot hold back the leader for long, and caught up again by snapshot after a hold-off that grows if
  it keeps falling behind.
- **Lanes split the streams over several connections**, so a snapshot or a slow stream holds back only its own lane's
  streams, and a follower applies the lanes in parallel. Sink writer threads let a follower keep receiving while its
  storage writes.
- **Your election decides who leads.** A node leads or follows when told, with the epoch your election gives it. Each
  `lead()` is a new leadership, even at an epoch used before, so a follower is never resumed from a log it did not
  follow.
  `handOver` before a clean stop tells the followers they hold everything, so the next leader takes over where this one
  left off.
- **A leader reachable over several networks.** A follower can be given all of its leader's addresses, and connects
  through the first that answers.
- **Safety checks on connecting.** Nodes of two different clusters refuse each other, two environments of one
  application included, and a follower refuses a stream the leader stores in the place it would write it.
- **Optional encryption with a secret the nodes share.** AES-CTR with HMAC-SHA256 by default, or AES-GCM, ChaCha20
  with HMAC or ChaCha20-Poly1305, through any JCE provider. Each connection derives its own keys. `PREFERRED` eases
  turning encryption on one node at a time; `REQUIRED` refuses a peer that does not encrypt.
- **Monitoring.** The leader sees each follower lane: connected, in sync, lag, writes pending, unsent bytes, refused
  streams, drops, snapshots, writes held by a full log, ack timeouts and closed connections by reason. A follower
  sees whether it is connected, encrypted and in sync, and counts its reconnects, what it received and its closed
  connections. A metrics callback times the followers' lag, ack round trip and catch up, and the writes held by a
  full log.
- **No allocation when writing.** Appending a record and sending it allocate nothing, and neither does the default
  cipher.
- **`replicos-testkit`** helps test an application's own streams and sinks: it feeds a follower the frames a leader
  would send and records what the follower answers.
- **Java 11 or later.**

[0.9.0]: https://github.com/lolaf-org/replicos/releases/tag/v0.9.0
