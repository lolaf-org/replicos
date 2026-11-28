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
package org.lolaf.replicos.leader;

import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWriter.MessageSentCallback;
import org.lolaf.replicos.CatchUp;
import org.lolaf.replicos.encryption.ConnectionEncryptionKeys;
import org.lolaf.replicos.encryption.KeyExchange;
import org.lolaf.replicos.journal.Journal;
import org.lolaf.replicos.journal.JournalStream;
import org.lolaf.replicos.spi.ReplicationMetrics;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.spi.StreamReplicationMode;
import org.lolaf.replicos.spi.TestingReplicationClock;
import org.lolaf.replicos.testkit.RecordingHandler;
import org.lolaf.replicos.testkit.TestingKeyExchanges;
import org.lolaf.replicos.wire.*;
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * A follower connection over a mocked IO session, whose frames sent are decoded as a follower would.
 */
abstract class FollowerConnectionHarness {

    static final long EPOCH = 7;
    static final long LEADERSHIP = 41;
    static final int MESSAGE_BYTES = 1_000;
    private static final ByteBuffer NO_PROOF = ByteBuffer.allocate(0);
    final IOSession ioSession;
    final RecordingHandler sent;
    /**
     * Whether each RECORD and SNAPSHOT_RECORD sent is flagged plaintext.
     */
    final List<Boolean> sentPlaintext;
    /**
     * Every byte sent, as it went on the socket.
     */
    final List<byte[]> sentBytes;
    /**
     * The batches sent, whose write betty has not reported yet while {@link #holdingWrites}.
     */
    final Queue<Runnable> unwrittenBatches;
    final AtomicInteger batchesSent;
    /**
     * The sender's clock in the connections {@link #connectionStallingAfter} builds.
     */
    final TestingReplicationClock senderClock;
    final AtomicLong senderIdles;
    ReplicationStats stats;
    volatile boolean holdingWrites;
    KeyExchange keyExchange;
    Journal journal;
    FollowerConnection connection;
    private ConnectionEncryptionKeys openedKeys;
    private FrameCipher opener;
    private boolean leaderAccepted;

    FollowerConnectionHarness() {
        this.ioSession = mock(IOSession.class);
        this.sent = new RecordingHandler(Journal.CLUSTER_ID);
        this.sentPlaintext = new CopyOnWriteArrayList<>();
        this.sentBytes = new CopyOnWriteArrayList<>();
        this.stats = new ReplicationStats(new SlowFollowers(Duration.ZERO, new TestingReplicationClock()), ReplicationMetrics.NONE);
        this.unwrittenBatches = new ConcurrentLinkedQueue<>();
        this.batchesSent = new AtomicInteger();
        this.senderClock = new TestingReplicationClock();
        this.senderIdles = new AtomicLong();
        this.keyExchange = TestingKeyExchanges.CLEAR;
        this.journal = Journal.ofCapacity(1 << 16);
        this.connection = connection();
        this.opener = FrameCipher.CLEAR;
        senderClock.toAdvanceableTime(Instant.parse("2026-11-01T10:00:00Z"), 0);
        when(ioSession.borrow(anyInt())).then(invocation -> ByteBuffer.allocate(invocation.getArgument(0)));
        // as betty on the IO thread, which every caller here stands for
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(ioSession).processTask(any(Runnable.class), any());
        doAnswer(invocation -> {
            ByteBuffer frames = invocation.getArgument(0);
            decodeAll(frames.duplicate().flip());
            return null;
        }).when(ioSession).send(any(ByteBuffer.class), anyBoolean());
        doAnswer(invocation -> {
            ByteBuffer frames = invocation.getArgument(0);
            MessageSentCallback<Object> callback = invocation.getArgument(2);
            if (!holdingWrites) {
                decodeAll(frames.duplicate().flip());
            }
            batchesSent.incrementAndGet();
            Runnable written = () -> {
                // as betty: flipped for the callback, its position and limit restored once it returns
                frames.flip();
                int position = frames.position();
                int limit = frames.limit();
                callback.onMessageWriteCallback(frames, null, invocation.getArgument(1));
                frames.limit(limit).position(position);
            };
            if (holdingWrites) {
                unwrittenBatches.add(written);
            } else {
                written.run();
            }
            return null;
        }).when(ioSession).send(any(ByteBuffer.class), any(), any(), anyBoolean());
    }

    /**
     * The follower's answer for a stream, catching up by full copy.
     */
    static void answer(FollowerConnection connection, int streamKey, StreamAnswer answer) {
        connection.onStreamAnswer(EPOCH, streamKey, answer, CatchUpCheck.FULL_COPY, -1, NO_PROOF, 0, 0);
    }

    /**
     * The follower needs a snapshot of the stream and proves it holds {@code texts}, its last records, the last at
     * {@code lastPosition}.
     */
    static void answerHolding(FollowerConnection connection, int streamKey, int records, long lastPosition,
                              String... texts) {
        RecordDigest digest = new RecordDigest();
        for (int i = 0; i < texts.length; i++) {
            byte[] payload = texts[i].getBytes(StandardCharsets.US_ASCII);
            digest.onRecord(JournalStream.TEXT, lastPosition - texts.length + 1 + i, ByteBuffer.wrap(payload), 0,
                    payload.length);
        }
        ByteBuffer proof = ByteBuffer.allocate(RecordDigest.PROOF_BYTES);
        int proofLength = digest.writeProof(records, proof, 0);
        connection.onStreamAnswer(EPOCH, streamKey, StreamAnswer.SNAPSHOT_NEEDED, CatchUpCheck.LAST_RECORDS,
                lastPosition, proof, 0, proofLength);
    }

    /**
     * {@code count} entries without a payload, all one length.
     */
    static void appendEmpty(JournalStream stream, int count) {
        for (int i = 0; i < count; i++) {
            stream.append("");
        }
    }

    FollowerConnection connection() {
        return connection(CatchUp.fullCopy());
    }

    FollowerConnection connection(CatchUp catchUp) {
        return new FollowerConnection(ioSession, journal.settings().heartbeatInterval(Duration.ofMillis(100)).build(),
                journal.getStreams(), journal.getLogs(), () -> EPOCH, LEADERSHIP, "leader-a", catchUp, stats, loop -> {
        }, new TestingReplicationClock(), keyExchange, new Followers(new TestingReplicationClock()));
    }

    JournalStream stream(String name) {
        return journal.add(name, StreamReplicationMode.SYNC_APPLIED);
    }

    /**
     * As a follower would, refusing a frame longer than the log's longest record and its tag, and opening the frames
     * of an encrypted connection.
     */
    void decodeAll(ByteBuffer frames) {
        byte[] bytes = new byte[frames.remaining()];
        frames.duplicate().get(bytes);
        sentBytes.add(bytes);
        FrameDecoder decoder = new FrameDecoder(journal.getLogs().get(0).getMaxRecordFrameBytes() + WireFormat.TAG_BYTES);
        int index = frames.position();
        int consumed;
        while (index < frames.limit() && (consumed = decoder.decode(frames, index, frames.limit() - index, sent, openerOf(frames, index))) > 0) {
            recordPlaintext(frames, index);
            index += consumed;
        }
    }

    /**
     * As the follower: the WELCOME clear, and a CLOSE before the leader's first sealed frame, a refusal.
     */
    private synchronized FrameCipher openerOf(ByteBuffer frames, int index) {
        FrameCipher keyed = opener();
        int type = frames.get(index + WireFormat.OFFSET_TYPE);
        if (type == FrameType.WELCOME.getCode() || type == FrameType.CLOSE.getCode() && !leaderAccepted) {
            return FrameCipher.CLEAR;
        }
        leaderAccepted = keyed != FrameCipher.CLEAR;
        return keyed;
    }

    /**
     * The follower's, one per connection, since it counts the frames it opens.
     */
    private synchronized FrameCipher opener() {
        ConnectionEncryptionKeys keys = connection.getEncryptionKeys();
        if (keys != openedKeys) {
            openedKeys = keys;
            leaderAccepted = false;
            opener = keys == null ? FrameCipher.CLEAR : keyExchange.newOpener(keys.getLeaderToFollower());
        }
        return opener;
    }

    /**
     * Whether the bytes sent hold {@code text}, as an observer of the socket would find it.
     */
    boolean sentBytesContain(String text) {
        byte[] wanted = text.getBytes(StandardCharsets.US_ASCII);
        for (byte[] bytes : sentBytes) {
            for (int i = 0; i + wanted.length <= bytes.length; i++) {
                if (Arrays.equals(bytes, i, i + wanted.length, wanted, 0, wanted.length)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void recordPlaintext(ByteBuffer frames, int frameIndex) {
        FrameType type = FrameType.from(frames.get(frameIndex + WireFormat.OFFSET_TYPE));
        if (type == FrameType.RECORD || type == FrameType.SNAPSHOT_RECORD) {
            sentPlaintext.add(FrameDecoder.isPlaintext(frames, frameIndex, type));
        }
    }

    void fill() {
        fill(1 << 20);
    }

    void fill(int batchBytes) {
        ByteBuffer batch = ByteBuffer.allocateDirect(batchBytes);
        int filled = connection.getSender().fillBatch(batch);
        decodeAll(batch.limit(filled));
    }

    /**
     * Starts the pending snapshot and sends its chunks until it ends, as the sender loop would with no live writes.
     */
    void snapshot() {
        connection.getSender().startPendingSnapshot();
        while (connection.getSender().isSnapshotInProgress() && connection.getState() == FollowerConnection.State.STREAMING) {
            connection.getSender().sendSnapshotChunk();
        }
    }

    FollowerConnection connectionStallingAfter(Duration stallTimeout, Thread[] sender) {
        IdleStrategy idles = countingIdles();
        return new FollowerConnection(ioSession, journal.settings().heartbeatInterval(Duration.ofHours(1))
                .followerStallTimeout(stallTimeout).senderIdleStrategy(() -> idles).build(), journal.getStreams(), journal.getLogs(),
                () -> EPOCH, LEADERSHIP, "leader-a", CatchUp.fullCopy(), stats, loop -> {
            sender[0] = new Thread(loop);
            sender[0].start();
        }, senderClock, keyExchange, new Followers(new TestingReplicationClock()));
    }

    private BackoffIdleStrategy countingIdles() {
        return new BackoffIdleStrategy() {
            @Override
            public void idle() {
                senderIdles.incrementAndGet();
                super.idle();
            }
        };
    }

    /**
     * Returns once the sender went through a whole turn of its loop, so it acted on everything done before the call.
     */
    void awaitSenderTurn() {
        long idles = senderIdles.get();
        await().until(() -> senderIdles.get() >= idles + 2);
    }

    FollowerConnection senderRunningConnection() {
        return senderRunningConnection(Integer.MAX_VALUE);
    }

    FollowerConnection senderRunningConnection(int snapshotChunkBytes) {
        IdleStrategy idles = countingIdles();
        return new FollowerConnection(ioSession, journal.settings().heartbeatInterval(Duration.ofHours(1))
                .snapshotChunkBytes(snapshotChunkBytes).senderIdleStrategy(() -> idles).build(), journal.getStreams(), journal.getLogs(),
                () -> EPOCH, LEADERSHIP, "leader-a", CatchUp.fullCopy(), stats, loop -> new Thread(loop).start(), new TestingReplicationClock(),
                keyExchange, new Followers(new TestingReplicationClock()));
    }

    /**
     * A stream holding {@code count} entries of {@code MESSAGE_BYTES}, which only a snapshot sends.
     */
    JournalStream streamHolding(String name, int count) {
        JournalStream stream = stream(name);
        stream.hold(count, MESSAGE_BYTES);
        return stream;
    }

    List<Long> snapshotPositions() {
        return sent.frames.stream().filter(frame -> frame.startsWith("SNAPSHOT_RECORD"))
                .map(frame -> Long.parseLong(frame.replaceAll(".* pos=(\\d+) .*", "$1"))).collect(Collectors.toList());
    }
}
