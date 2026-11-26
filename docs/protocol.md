# Replication protocol, version 1

How a leader replicates an application's streams to one follower over TCP: the frames, the handshake, the
acknowledgements, snapshots and encryption. The library never reads what it replicates: records, their content types
and a stream's state are the application's bytes. For the behaviour behind the frames, when a follower resumes and
when it is caught up by snapshot, read [how-it-works.md](how-it-works.md) first.

## Model

- **An application** is identified by a 4-byte id, the same on both nodes; a peer replicating another application is
  refused.
- **A stream** is a sequence of records the leader appends, each at a **position**, increasing between two resets.
  Streams have a key, given from 0 in registration order and never reused, a name the follower finds its own copy by,
  a replication mode and a storage identity.
- **A record** carries a content type (one byte, the application's), flags, its position and a payload.
- **A lane** is one TCP connection and one log. Stream `k` goes over lane `k` modulo the lane count, so a slow stream
  only holds back the streams of its lane. The follower opens lane 0 first; the leader's WELCOME gives the lane count,
  and the follower opens the others to match.
- **The log** of a lane holds its streams' records in the order they were appended, each at an increasing **log
  offset**, until the follower has them. It is bounded: a follower a whole log behind holds appends back, then is
  dropped.
- **An epoch** numbers leaderships; every frame carries the sender's. The source of epochs (an election) is outside
  the library.
- **A leadership** is 8 random bytes a node draws each time it starts leading. Log offsets mean nothing outside the
  leadership that wrote them, so a leader restarted at the same epoch never resumes a follower of its previous run.

## Frames

Big-endian throughout. A string is UTF-8, its length an unsigned short given in the fixed payload. Unused flag bits
and reserved fields are sent as zero and ignored on receipt.

### Header, 16 bytes

| offset | size | field |
|---|---|---|
| 0 | 4 | frame length, header and tag included |
| 4 | 1 | frame type |
| 5 | 1 | flags, per frame type |
| 6 | 2 | reserved |
| 8 | 8 | epoch of the sender |

The fixed payload follows, then the variable part, then a 16-byte tag on an encrypted connection.

### Frame types

| code | frame | direction | fixed payload, then variable part |
|---|---|---|---|
| 1 | HELLO | follower to leader | magic 4, version 2, node id length 2, last applied epoch 8, last applied leadership 8, last applied offset 8, lane 2, encryption mode 1, reserved 1, application id 4, nonce 16; node id |
| 2 | WELCOME | leader to follower | magic 4, version 2, leader id length 2, leadership 8, resume offset 8, lane count 2, encryption mode 1, reserved 1, application id 4, nonce 16; leader id |
| 3 | STREAM | leader to follower | stream key 4, replication mode 1, reserved 1, name length 2, storage identity length 2; name, storage identity |
| 4 | STREAM_ANSWER | follower to leader | stream key 4, answer 1, catch-up check 1, proof length 2, last position 8; proof |
| 5 | RECORD | leader to follower | log offset 8, record body 24; payload |
| 6 | SNAPSHOT_BEGIN | leader to follower | stream key 4, reserved 2, state length 2, snapshot offset 8, from position 8; state |
| 7 | SNAPSHOT_RECORD | leader to follower | record body 24; payload |
| 8 | SNAPSHOT_END | leader to follower | stream key 4, record count 4, boundary count 2, state length 2; state, boundaries |
| 9 | ACK | follower to leader | applied offset 8, pending writes 8 |
| 10 | HEARTBEAT | leader to follower | log tail offset 8 |
| 11 | CLOSE | either | reason 2, text length 2; text |
| 12 | RECEIVED | follower to leader | received offset 8 |
| 13 | STREAMS_ANNOUNCED | leader to follower | announced count 4, reserved 4 |
| 14 | KEY_CONFIRMATION | follower to leader | HMAC-SHA256 32 |

A frame of an unknown type closes the connection.

**Record body**, shared by RECORD and SNAPSHOT_RECORD: stream key 4, content type 1, flags 1, reserved 2, position 8,
payload length 4, reserved 4.

### Constants and codes

- **Magic** `0x5245504C` ('REPL'), **version** 1. A peer of another version is closed with `VERSION_MISMATCH`, never
  interpreted: any layout change bumps the version.
- **Header flags.** STREAM: `0x01` the stream has a storage identity. ACK: `0x01` in sync, the follower holds every
  stream of the lane the leader replicates.
- **Record flags.** `0x01` `MORE_FRAGMENTS`, `0x02` `CONTINUED_FRAGMENT`, `0x04` `PLAINTEXT`, `0x08` `RESET`; see
  [Records](#records).
- **Replication mode.** 0 `NONE` (announced, not replicated), 1 `ASYNC`, 2 `SYNC_APPLIED`, 3 `SYNC_RECEIVED`.
- **Encryption mode.** 0 `DISABLED`, 1 `PREFERRED`, 2 `REQUIRED`.
- **Resume offset** -1: the follower's position is not in the leader's log.
- **Stream answers.**

  | code | answer | meaning |
  |---|---|---|
  | 0 | `REPLICATING` | in sync |
  | 1 | `SHARED_STORAGE` | refused: the follower stores the stream where the leader does |
  | 2 | `UNKNOWN_STORAGE` | refused: the leader gave no storage identity, so sharing cannot be ruled out |
  | 3 | `UNKNOWN_STREAM` | refused: the follower's configuration does not know the stream |
  | 4 | `SNAPSHOT_NEEDED` | replicated, but missing history: the leader sends a snapshot |
  | 5 | `NOT_REPLICATED` | the leader announced it with mode `NONE` |
  | 6 | `MODE_MISMATCH` | refused: the follower's configuration gives it another mode |

- **Close reasons.**

  | code | reason | sent by, when |
  |---|---|---|
  | 0 | `SHUTDOWN` | either, stopping; from the leader while streaming, a [hand over](#hand-over) |
  | 1 | `VERSION_MISMATCH` | either, on HELLO or WELCOME |
  | 2 | `STALE_EPOCH` | the leader on a HELLO of a newer epoch; the follower on a WELCOME of an older epoch, or a frame whose epoch differs from the WELCOME's |
  | 3 | `PROTOCOL_ERROR` | either: a frame out of place or malformed |
  | 4 | `SNAPSHOT_FAILED` | leader: a stream could not be read for its snapshot |
  | 5 | `APPLY_FAILED` | follower: it could not apply what it received |
  | 6 | `STALLED` | leader: no ACK for the stall timeout while one was owed |
  | 7 | `TOO_SLOW` | leader: an append waited the log full timeout for this follower |
  | 8 | `RETRY_LATER` | leader: a follower dropped as too slow is held off before it may resync |
  | 9 | `NO_SUCH_LANE` | leader: HELLO for a lane past its count |
  | 10 | `KEY_MISMATCH` | leader: KEY_CONFIRMATION does not verify |
  | 11 | `ENCRYPTION_REQUIRED` | either: one requires encryption, the other has it disabled |
  | 12 | `APPLICATION_MISMATCH` | either: another application id |
  | 13 | `LEADER_MISMATCH` | follower: a lane's WELCOME names another leader than lane 0 follows, as when its leader addresses mix two nodes |

  A code the peer does not know reads as `UNKNOWN` (0xFFFF, never sent) rather than refusing the frame. The text is at
  most 1024 characters, for the logs.

## Connecting

Per lane:

1. The follower sends **HELLO**: its epoch, the last epoch, leadership and log offset it applied on this lane (offset
   -1 when none), the lane, its encryption mode and a nonce (16 random bytes when it encrypts, zeros otherwise).
2. The leader checks the version, the application, that the follower's epoch is not newer than its own (the leader
   would be deposed) and the lane, then [negotiates encryption](#negotiation). A follower dropped as too slow and
   still held off gets `RETRY_LATER`.
3. The leader sends **WELCOME**: its epoch, its leadership, the lane count, its mode, its nonce and the **resume
   offset**. It resumes when the follower's last applied epoch and leadership are the leader's current ones and the
   record at its last applied offset is still in the log; the resume offset is then where the log reads next.
   Otherwise it is -1, and every stream of the lane needs a snapshot.
4. When both encrypt, the follower sends **KEY_CONFIRMATION**, and the leader streams only once it verified it.
5. The leader announces each stream of the lane with a **STREAM**, then **STREAMS_ANNOUNCED** with the count it sent,
   which the follower checks. A stream registered or reconfigured later is announced again with a STREAM at any time.
6. The follower answers each STREAM with a **STREAM_ANSWER**. A stream it held in sync over a resumed connection is
   `REPLICATING`; one it replicates but does not hold whole is `SNAPSHOT_NEEDED`; the refusals are configuration
   errors the leader reports. A stream refused as unknown or for its mode is answered again once the follower's
   configuration changes. The answer also carries the follower's catch-up check, the last position it holds of the
   stream (-1 for none) and the check's proof of it, from which the leader picks where a snapshot starts:
   - **0, full copy**: no proof; the snapshot copies the whole stream.
   - **1, last records**: for a stream needing a snapshot, the follower's last records, at most the count it is
     configured with, proved by records 4 (that count), first position 8 and SHA-256 32: over each record from the
     first position to the last, its position 8, content type 1, payload length 4 and payload. The leader digests its
     own records over those positions: the same digest starts the snapshot after the follower's last position,
     anything else at 0. No proof, as from a follower holding nothing, gets the whole stream.
   - **2, reserved** for a check by the history of the leaderships that wrote each stream, see
     [Reserved for an exact catch-up](#reserved-for-an-exact-catch-up). A leader reads a check it does not know as a
     mismatch.

   A follower whose check or count is not the leader's gets the whole stream, and the leader logs it once per
   connection. A follower closes with `PROTOCOL_ERROR` a snapshot starting past its last position + 1.

Every frame after the WELCOME must carry the WELCOME's epoch; any other closes the connection with `STALE_EPOCH`. A
follower whose connection drops reconnects, sending its new last applied offset.

## Records

The leader sends the lane's log in order as RECORD frames, each with its log offset. A record's flags:

- **`MORE_FRAGMENTS`, `CONTINUED_FRAGMENT`.** A payload longer than one frame goes as consecutive records of its
  stream, other streams' records possibly between them: the first fragment has `MORE_FRAGMENTS`, the middle ones
  both, the last `CONTINUED_FRAGMENT` alone, so a last fragment never reads as a whole payload. A fragment out of
  sequence is a protocol error. The follower applies the payload once whole.
- **`PLAINTEXT`.** On an encrypted connection the payload goes unencrypted, still authenticated. Chosen by the
  application per record.
- **`RESET`.** The stream starts over at this record. It ends a snapshot of the stream in progress.

With nothing to send, the leader sends a **HEARTBEAT** carrying its log tail offset.

## Acknowledgements

- **ACK** carries the offset of the last record the follower applied (handed to the application and written), whether
  the lane is in sync, and the writes it accepted but has not made durable yet, for the leader's monitoring. The
  follower sends one at the end of every read that brought records, a heartbeat or a snapshot's end, so a write
  waiting on it costs one round trip, not a timer.
- **RECEIVED** carries the offset of the last record the follower holds in memory, sent before it writes them, for
  `SYNC_RECEIVED`.
- **What waits.** An append of a `SYNC_APPLIED` stream waits for an ACK at or past its offset, of a `SYNC_RECEIVED`
  stream for a RECEIVED or ACK at or past it, from a follower holding the stream in sync; `ASYNC` waits for nothing.
  A wait has a timeout, after which the append goes on and later ones do not wait for that follower until it caught
  up. The leader closes a follower that owes an ACK and stays silent for its stall timeout with `STALLED`.

## Snapshots

A stream the follower answered `SNAPSHOT_NEEDED`, or one the leader resyncs, is copied by snapshot, one stream at a
time per lane, in chunks between the lane's live batches so the other streams keep replicating:

1. **SNAPSHOT_BEGIN** carries the stream's state, the log offset the snapshot starts at and the first position it
   copies. The follower discards its copy of the stream from that position on, all of it from 0.
2. **SNAPSHOT_RECORD** frames carry the stream's records by position, fragmented as RECORD's.
3. **SNAPSHOT_END** carries the record count, which the follower checks against what it received, and the state when
   the snapshot caught up, which supersedes the one it began with. The follower answers `REPLICATING`, and the
   stream's `SYNC` appends wait for it from then on. The boundaries after the state, 16 bytes each (leadership 8,
   first position 8), are [reserved for check 2](#reserved-for-an-exact-catch-up): this version sends none and skips
   any it receives.

Meanwhile the leader skips the stream's records in the log, except a `RESET`, which ends the snapshot; from its end,
the stream's records come through the log again. The log may then bring a record the snapshot already held, which the
application may skip by its position. Before the snapshot begins, the follower applies none of the stream's records
the log brings: the snapshot copies them, and stored past what the follower holds they would leave its copy a gap.

The snapshot copies in rounds while the stream keeps being written: each round reads the positions from the last one
copied up to the last position the stream's state covers when the round starts, and the snapshot ends with a round
that fits one chunk. A round no shorter than the one before, the stream written faster than it is copied, is sent
whole. This relies on two things the application guarantees:

- **A record is readable from the application's storage before it is appended to the log**, so a record skipped from
  the log is always in a later round.
- **Positions increase between two resets**, so a round's range holds every record appended since the last.

The state is opaque bytes, at most 65535, from which the follower's application resumes the stream (for a sequence,
the next position it expects). This model fits an append-mostly log, not an arbitrary state machine needing a
point-in-time snapshot.

## Reserved for an exact catch-up

Version 1 leaves room for a third catch-up check, not built yet, so a later version can add it without a new protocol
version: a version 1 peer reads it as a check it does not know and copies the whole stream. It would work as Kafka's
leader epochs do. Each node keeps, durably and per stream, the first position each leadership wrote, so the leader
finds exactly where a follower's copy parts from its own: no blind spot, and a deposed leader caught up incrementally.

| Room | Where | Version 1 | Check 2 would carry |
|---|---|---|---|
| Check code 2 | STREAM_ANSWER | never sent; read as unknown, a full copy | the follower proving its copy by its leadership history |
| The proof | STREAM_ANSWER | check 1's digest | the leadership of the follower's record at its last position, 8 bytes |
| Boundary count 2, then the boundaries after the state, 16 bytes each | SNAPSHOT_END | count 0; any boundary received is skipped | the leader's history over the positions copied (leadership 8, first position 8), replacing the follower's from the snapshot's start |
| `ReplicaSink.lastPosition()` | the Java SPI, default -1 | never called | the copy's last durable position, claimed without reading its records back |

The check: the follower claims its last position `p` and the leadership `L` of its record there. The leader starts the
snapshot after `min(p, the end of L in its own history)`, and at 0 for a leadership it does not know. A history keeps
at most 1024 boundaries per stream, the oldest dropped, and a reset starts it again.

It is not built because of when a boundary would be written. An application stores a record before it appends it, so
a leader crashing between its first store of a leadership and that leadership's boundary holds records its history
gives to the previous leadership, which the check would then keep. Kafka has no such window, its log being its
storage. Closing it needs the application to announce a position before storing it, as a
`StreamWriter.beforeStore(position)` that writes the boundary durably first: an API change for a later version.

## Hand over

A leader stopping cleanly sends every record of the log, then **CLOSE** `SHUTDOWN`, and waits for the follower to
hang up. A follower receiving it while streaming, at the WELCOME's epoch, knows it holds everything the leader wrote
on that lane, so it can take over without assuming lost writes.

## Encryption

### Negotiation

| this node | the peer | the connection |
|---|---|---|
| `DISABLED` | `DISABLED` or `PREFERRED` | clear |
| `PREFERRED` | `DISABLED` | clear, an error logged on this node |
| `REQUIRED` | `DISABLED` | refused, `ENCRYPTION_REQUIRED` |
| `PREFERRED` or `REQUIRED` | `PREFERRED` or `REQUIRED` | encrypted; a different secret or cipher is refused, `KEY_MISMATCH` |

Only `REQUIRED` stops a downgrade: the modes travel in the clear HELLO and WELCOME, so an attacker on the network can
rewrite a `PREFERRED` peer's to `DISABLED`.

### Keys

Each connection derives its own keys with HKDF-SHA256 (RFC 5869):

- **Extract:** salt the follower's nonce then the leader's, input the shared secret.
- **Expand**, each with its info label in UTF-8:

  | key | info | bytes |
  |---|---|---|
  | key confirmation | `replication key confirmation` | 32 |
  | cipher key, per direction | `replication <direction> cipher` | the cipher's key size |
  | MAC key, per direction | `replication <direction> mac` | 32 |
  | IV, per direction | `replication <direction> iv` | 16 |

  `<direction>` is `leader-to-follower` or `follower-to-leader`.

**KEY_CONFIRMATION** is the HMAC-SHA256, under the key confirmation key, of the label
`replication key confirmation v1`, then the HELLO as the follower sent it, the WELCOME as the leader sent it, and the
cipher's description (`<cipher> <key bits>`, then ` <mac>` for a stream cipher, as `AES/CTR/NoPadding 256 HmacSHA256`),
each of these three preceded by its length in 4 bytes. It covers every field of both frames, so a frame altered in
transit, a different cipher or a different secret fails it alike. The leader compares it in constant time.

### Sealed frames

From the follower, every frame after its KEY_CONFIRMATION is sealed; from the leader, every frame from the first it
sends once it verified it. The leader's refusals before then go unsealed, since the keys may differ: forging one only
refuses the connection.

Sealing a frame sets its length to include a 16-byte tag, appended. The header and fixed payload are authenticated in
clear; the variable part is encrypted too, except in a `PLAINTEXT` record, where nothing is. Each direction counts its
sealed frames from 0, and the count goes into each tag, so a frame altered, replayed, dropped or reordered fails to
verify and closes the connection.

- **A stream cipher** (`AES/CTR/NoPadding`, `ChaCha20`) runs one keystream over the direction's encrypted bytes, in
  segments of 2^32 bytes. Each segment's IV is the direction's IV with the segment number XORed in: for AES-CTR, a
  counter block of the IV's first 8 bytes XORed with the number, then 8 zero bytes; for ChaCha20, a 12-byte nonce of
  the IV's first 12 bytes, the number XORed into bytes 4 to 11, counter 0. The tag is the MAC (`HmacSHA256` by
  default) of the frame number in 8 bytes, then the frame as sent, cut to 16 bytes.
- **An AEAD** (`AES/GCM/NoPadding`, `ChaCha20-Poly1305`) seals each frame under a 12-byte nonce: the IV's first 12
  bytes, the frame number XORed into the last 8. The header and fixed payload are the additional data, the variable
  part the plaintext, its 16-byte tag the frame's.

Numbers in a nonce, IV or tag are big-endian.
