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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.replicos.CatchUp;
import org.lolaf.replicos.EncryptionMode;
import org.lolaf.replicos.FollowerLaneStatus;
import org.lolaf.replicos.LeaderSettings;
import org.lolaf.replicos.encryption.ConnectionEncryptionKeys;
import org.lolaf.replicos.encryption.KeyAgreement;
import org.lolaf.replicos.encryption.KeyExchange;
import org.lolaf.replicos.encryption.Negotiation;
import org.lolaf.replicos.log.ReplicationLog;
import org.lolaf.replicos.log.ReplicationLogs;
import org.lolaf.replicos.spi.ReplicationClock;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.wire.*;
import org.lolaf.ringos.Deadline;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * One follower's connection for one lane: the handshake, the frames the follower sends back, and the close.
 *
 * <p>Frames from the follower arrive on the connection's IO thread; a {@link FollowerSender} streams the log to it
 * from a thread of its own. While streaming, its reader holds appends back rather than be lapped; a writer that
 * waited the log full timeout for it drops it, and it reconnects to be caught up.
 */
@Slf4j
public final class FollowerConnection implements FrameHandler {

    private static final Duration CLOSE_FLUSH = Duration.ofSeconds(1);
    private static final AtomicReferenceFieldUpdater<FollowerConnection, State> STATE =
            AtomicReferenceFieldUpdater.newUpdater(FollowerConnection.class, State.class, "state");
    private static final AtomicReferenceFieldUpdater<FollowerConnection, PendingClose> PENDING_CLOSE =
            AtomicReferenceFieldUpdater.newUpdater(FollowerConnection.class, PendingClose.class, "pendingClose");
    private final IOSession ioSession;
    private final String address;
    private final ReplicationLogs logs;
    private final StreamRegistry streams;
    /**
     * This node's current election epoch, stamped on every frame it sends.
     */
    private final LongSupplier nodeEpoch;
    /**
     * Sent in WELCOME and expected back in a HELLO that resumes: offsets in this node's log mean nothing to a follower
     * of another leadership, even of the same epoch, as a leader restarted with its epoch.
     */
    private final long leadership;
    private final String leaderId;
    private final ReplicationStats stats;
    private final StreamSyncs syncs;
    @Getter
    private final FollowerAcks acks;
    @Getter(AccessLevel.PACKAGE)
    private final FollowerSender sender;
    private final KeyExchange keyExchange;
    private final KeyAgreement keyAgreement;
    private final Followers followers;
    private final Set<String> syncFollowers;
    /**
     * Two heartbeats: a follower that has not acked for that long while its node id connects again is gone, not alive
     * beside a second node given its id.
     */
    private final long reconnectSilenceNanos;
    private final AtomicBoolean closeSent;
    private final ReplicationClock clock;
    /**
     * When the lane left sync, at the HELLO, a stream announced or a re-sync, -1 while it is in sync: the start of the
     * catch-up being timed.
     */
    private final AtomicLong outOfSyncSinceNanos;
    /**
     * The log of the lane the HELLO named, null before it.
     */
    private volatile @Nullable ReplicationLog replicationLog;
    /**
     * Opened by the HELLO, holding the lane's appends back until the connection closes; null before it.
     */
    private volatile ReplicationLog.@Nullable Reader reader;
    @Getter
    private volatile int lane;
    /**
     * Opens the follower's frames once its KEY_CONFIRMATION verified; IO thread only.
     */
    @Getter(AccessLevel.PACKAGE)
    private FrameCipher answerOpener;
    @Getter
    private volatile State state;
    private volatile boolean followerHungUp;
    @Getter
    private volatile @Nullable String followerId;
    /**
     * Set by the HELLO, before the state turns streaming: plain, since the other threads read it after the state.
     */
    @Getter
    private boolean syncFollower;
    /**
     * This connection holds its follower's node id on its lane, see {@link Followers#claim}.
     */
    private volatile boolean claimed;
    private volatile long claimedNanos;
    /**
     * Recorded by the first close; the IO thread sends it once the sender stopped, sealed after the sender's last frame.
     */
    private volatile @Nullable PendingClose pendingClose;
    /**
     * How long the lane's last return to sync took, -1 before the first; written on the IO thread, read by the status.
     */
    private volatile long catchUpNanos;

    /**
     * @param senderLauncher starts the loop streaming the log, on a thread of its own
     */
    FollowerConnection(IOSession ioSession, LeaderSettings settings, StreamRegistry streams, ReplicationLogs logs,
                       LongSupplier nodeEpoch, long leadership, String leaderId, CatchUp catchUp, ReplicationStats stats,
                       Consumer<Runnable> senderLauncher, ReplicationClock clock, KeyExchange keyExchange,
                       Followers followers) {
        this.syncs = new StreamSyncs();
        this.answerOpener = FrameCipher.CLEAR;
        this.closeSent = new AtomicBoolean();
        this.clock = clock;
        this.catchUpNanos = -1;
        this.outOfSyncSinceNanos = new AtomicLong(-1);
        this.state = State.AWAITING_HELLO;
        this.ioSession = ioSession;
        this.address = String.valueOf(ioSession.getSocketAddress());
        this.logs = logs;
        this.streams = streams;
        this.syncFollowers = settings.getSyncFollowers();
        this.reconnectSilenceNanos = 2 * settings.getHeartbeatInterval().toNanos();
        this.followers = followers;
        this.nodeEpoch = nodeEpoch;
        this.leadership = leadership;
        this.leaderId = leaderId;
        this.stats = stats;
        this.keyExchange = keyExchange;
        this.keyAgreement = KeyAgreement.ofLeader(keyExchange);
        this.acks = new FollowerAcks(stats, clock);
        this.sender = new FollowerSender(this, ioSession, settings, streams, syncs, acks, nodeEpoch,
                logs.getMaxRecordFrameBytes(), catchUp, stats, senderLauncher, clock);
    }

    @Override
    public void onHello(long epoch, int version, int clusterId, String nodeId, long lastAppliedEpoch,
                        long lastAppliedLeadership, long lastAppliedOffset, int lane, EncryptionMode followerMode,
                        byte[] followerNonce) {
        followerId = nodeId;
        long currentEpoch = nodeEpoch.getAsLong();
        if (!acceptsHello(epoch, version, clusterId, lane, currentEpoch)) {
            return;
        }
        this.lane = lane;
        if (!followers.claim(this, nodeId, lane)) {
            close(CloseReason.DUPLICATE_NODE_ID, "follower " + nodeId + " is connected on lane " + lane + " already");
            return;
        }
        syncFollower = syncFollowers.isEmpty() || syncFollowers.contains(nodeId);
        Negotiation negotiation = keyExchange.negotiate(followerMode);
        if (negotiation == Negotiation.REFUSED) {
            close(CloseReason.ENCRYPTION_REQUIRED, "this leader's encryption is " + keyExchange.getMode()
                    + ", the follower's " + followerMode);
            return;
        }
        replicationLog = logs.get(lane);
        Optional<Instant> heldOffUntil = stats.getSlowFollowers().heldOffUntil(nodeId, lane);
        if (heldOffUntil.isPresent()) {
            close(CloseReason.RETRY_LATER, "dropped as too slow, held off until " + heldOffUntil.get());
            return;
        }
        reader = replicationLog.newReader(syncFollower, this::droppedAsTooSlow);
        boolean resumed = lastAppliedEpoch == currentEpoch && lastAppliedLeadership == leadership
                && resume(lastAppliedOffset);
        if (!resumed) {
            reader.seekToTail();
        }
        acks.hello(nodeId, lane, resumed ? lastAppliedOffset : -1);
        boolean encrypted = negotiation == Negotiation.ENCRYPTED;
        // a writer may have dropped the reader already
        if (!STATE.compareAndSet(this, State.AWAITING_HELLO, encrypted ? State.AWAITING_KEY_CONFIRMATION : State.STREAMING)) {
            if (state == State.DROPPED_AS_TOO_SLOW) {
                closeDropped();
            }
            return;
        }
        outOfSyncSinceNanos.set(clock.nanoTime());
        log.info("Follower {} connected lane {} of {} at epoch {}, {}", nodeId, lane, logs.count(), currentEpoch,
                resumed ? "resuming after offset " + lastAppliedOffset : "needing a snapshot");
        if (negotiation == Negotiation.CLEAR_AGAINST_PREFERENCE) {
            log.error("Replicating in clear to follower {} lane {}: this leader prefers encryption, the follower has it "
                    + "disabled", nodeId, lane);
        }
        long resumeOffset = resumed ? reader.getOffset() : WireFormat.NO_RESUME;
        byte @Nullable [] helloToConfirm = encrypted ? FrameEncoder.helloBytes(epoch, clusterId, nodeId, lastAppliedEpoch,
                lastAppliedLeadership, lastAppliedOffset, lane, followerMode, followerNonce) : null;
        sender.start(replicationLog, reader, sendWelcome(currentEpoch, resumeOffset, helloToConfirm, followerNonce));
    }

    private boolean acceptsHello(long epoch, int version, int clusterId, int lane, long currentEpoch) {
        if (state != State.AWAITING_HELLO) {
            close(CloseReason.PROTOCOL_ERROR, "a second HELLO");
            return false;
        }
        if (version != WireFormat.VERSION) {
            close(CloseReason.VERSION_MISMATCH, "leader speaks version " + WireFormat.VERSION + ", follower " + version);
            return false;
        }
        if (clusterId != streams.getClusterId()) {
            close(CloseReason.CLUSTER_MISMATCH, "leader is of cluster "
                    + Integer.toHexString(streams.getClusterId()) + ", follower " + Integer.toHexString(clusterId));
            return false;
        }
        if (epoch > currentEpoch) {
            close(CloseReason.STALE_EPOCH, "follower is at epoch " + epoch + ", this leader at " + currentEpoch);
            return false;
        }
        if (lane >= logs.count()) {
            close(CloseReason.NO_SUCH_LANE, "lane " + lane + " of " + logs.count());
            return false;
        }
        return true;
    }

    private FrameCipher sendWelcome(long currentEpoch, long resumeOffset, byte @Nullable [] helloToConfirm,
                                    byte[] followerNonce) {
        ByteBuffer frame = ioSession.borrow(FrameEncoder.bytesWith(FrameType.WELCOME, leaderId));
        int length = FrameEncoder.welcome(frame.clear(), 0, currentEpoch, streams.getClusterId(), leaderId,
                leadership, resumeOffset, logs.count(), keyExchange.getMode(), keyAgreement.getNonce());
        FrameCipher sealer = FrameCipher.CLEAR;
        if (helloToConfirm != null) {
            keyAgreement.hello(helloToConfirm);
            keyAgreement.agree(followerNonce, FrameDecoder.bytes(frame, 0, length));
            sealer = keyAgreement.newSealer();
        }
        ioSession.send(BatchBuffers.written(frame, length), true);
        return sealer;
    }

    @Override
    public void onKeyConfirmation(long epoch, byte[] confirmation) {
        if (state == State.DROPPED_AS_TOO_SLOW) {
            return;
        }
        if (state != State.AWAITING_KEY_CONFIRMATION || keyAgreement.getKeys() == null) {
            close(CloseReason.PROTOCOL_ERROR, "a KEY_CONFIRMATION on a connection not waiting for one");
            return;
        }
        if (!keyAgreement.confirms(confirmation)) {
            close(CloseReason.KEY_MISMATCH, "the follower's secret or cipher differ from this leader's "
                    + keyExchange.describeCipher());
            return;
        }
        answerOpener = keyAgreement.newOpener();
        if (STATE.compareAndSet(this, State.AWAITING_KEY_CONFIRMATION, State.STREAMING)) {
            log.info("Replicating encrypted to follower {} lane {}, {}", followerId, lane, keyExchange.describeCipher());
            sender.wakeup();
        }
    }

    /**
     * Positions the reader after the follower's last applied record, if that record is still in the log.
     */
    private boolean resume(long lastAppliedOffset) {
        if (lastAppliedOffset < 0 || !reader.seek(lastAppliedOffset)) {
            return false;
        }
        ByteBuffer skipped = ByteBuffer.allocate(logs.getMaxRecordFrameBytes());
        return reader.readInto(skipped, 0) >= 0;
    }

    @Override
    public void onReceived(long epoch, long receivedOffset) {
        acks.onReceived(receivedOffset);
    }

    @Override
    public void onAck(long epoch, long appliedOffset, boolean inSync, long pendingWrites) {
        acks.onAck(appliedOffset, inSync, pendingWrites);
    }

    void wakeSender() {
        sender.wakeup();
    }

    /**
     * How long the follower has been behind a write it has not applied yet, 0 when it is not, or not streaming.
     */
    public long getLagNanos() {
        return state == State.STREAMING ? acks.getLagNanos(clock.nanoTime()) : 0;
    }

    /**
     * The follower holds everything of the stream up to its last ack, so a {@code SYNC} send of the stream waits
     * for its ack. Allocates nothing.
     */
    public boolean isStreamInSync(int streamKey) {
        return state == State.STREAMING && syncs.get(streamKey) == StreamSyncState.IN_SYNC;
    }

    /**
     * One a {@code SYNC} append waits for: a sync follower holding the stream in sync.
     */
    public boolean isSyncFollowerInSync(int streamKey) {
        return isStreamInSync(streamKey) && syncFollower;
    }

    /**
     * A sync follower holding every stream of the lane in sync: a failover to it loses nothing acknowledged.
     *
     * @param streamCount as {@link #isLaneInSync}
     */
    public boolean isSyncFollowerLaneInSync(int streamCount) {
        return isLaneInSync(streamCount) && syncFollower;
    }

    @Nullable StreamSyncState getSyncState(int streamKey) {
        return syncs.get(streamKey);
    }

    @Override
    public void onStreamAnswer(long epoch, int streamKey, StreamAnswer answer, CatchUpCheck check, long lastPosition,
                               ByteBuffer buffer, int proofIndex, int proofLength) {
        if (syncs.get(streamKey) == null) {
            close(CloseReason.PROTOCOL_ERROR, "a STREAM_ANSWER for stream key " + streamKey + ", never announced");
            return;
        }
        syncs.answered(streamKey, answer);
        syncs.claimed(streamKey, check, lastPosition, buffer, proofIndex, proofLength);
        switch (answer) {
            case REPLICATING:
                syncs.set(streamKey, StreamSyncState.IN_SYNC);
                break;
            case SNAPSHOT_NEEDED:
                syncs.set(streamKey, StreamSyncState.SNAPSHOT_PENDING);
                sender.requestSnapshot();
                break;
            case NOT_REPLICATED:
                syncs.set(streamKey, StreamSyncState.NOT_REPLICATED);
                break;
            default:
                syncs.set(streamKey, StreamSyncState.NOT_REPLICATED);
                log.warn("Follower {} does not replicate stream {}: {}", followerId,
                        streams.get(streamKey).getStreamName(), answer);
        }
        timeCatchUp();
    }

    /**
     * A stream of the lane leaves sync: times the lane's way back, unless a catch-up is timed already. Any thread.
     */
    void leftSync(int streamKey) {
        if (streams.get(streamKey).isReplicated()) {
            outOfSyncSinceNanos.compareAndSet(-1, clock.nanoTime());
        }
    }

    private void timeCatchUp() {
        long since = outOfSyncSinceNanos.get();
        if (since >= 0 && isLaneInSync(streams.count()) && outOfSyncSinceNanos.compareAndSet(since, -1)) {
            catchUpNanos = clock.nanoTime() - since;
            if (!isLaneInSync(streams.count())) {
                // a stream left sync again between the check and the reset, its leftSync finding a catch-up timed
                outOfSyncSinceNanos.compareAndSet(-1, clock.nanoTime());
            }
        }
    }

    /**
     * Sends the stream again by snapshot, which resets the follower's copy first. Only a copy the follower holds is
     * replaced: a stream it refuses or is still catching up on is left as it is.
     */
    void resync(int streamKey) {
        if (carries(streamKey) && syncs.getAnswer(streamKey) == StreamAnswer.REPLICATING
                && syncs.compareAndSet(streamKey, StreamSyncState.IN_SYNC, StreamSyncState.SNAPSHOT_PENDING)) {
            leftSync(streamKey);
            sender.requestSnapshot();
        }
    }

    public @Nullable StreamAnswer getStreamAnswer(int streamKey) {
        return syncs.getAnswer(streamKey);
    }

    /**
     * @param streamCount the streams the leader replicates, every one of which the follower must hold to be in sync
     */
    public FollowerLaneStatus getStatus(int streamCount) {
        return FollowerLaneStatus.builder()
                .nodeId(followerId)
                .lane(lane)
                .address(address)
                .connected(isConnected())
                .inSync(isLaneInSync(streamCount))
                .syncFollower(syncFollower)
                .snapshotInProgress(syncs.isSnapshotInProgress(streamCount))
                .appliedOffset(acks.getAppliedOffset())
                .pendingWrites(acks.getPendingWrites())
                .unsentBytes(getUnsentBytes())
                .lagMillis(TimeUnit.NANOSECONDS.toMillis(getLagNanos()))
                .catchUpMillis(catchUpNanos < 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(catchUpNanos))
                .cipher(isEncrypted() ? keyExchange.describeCipher() : null)
                .build();
    }

    public boolean isConnected() {
        return state == State.STREAMING;
    }

    void claimed() {
        claimedNanos = clock.nanoTime();
        claimed = true;
    }

    /**
     * Closes this connection as stalled when its follower has acked nothing for two heartbeats, as one reconnecting
     * before this leader saw its connection go: a second node given its id finds it acking, and is refused.
     *
     * @return whether the connection no longer holds its claim
     */
    boolean closeIfSilent() {
        long silentNanos = clock.nanoTime() - Math.max(claimedNanos, acks.getLastAckNanos());
        if (silentNanos <= reconnectSilenceNanos) {
            return false;
        }
        close(CloseReason.STALLED, "no ack for " + TimeUnit.NANOSECONDS.toMillis(silentNanos)
                + " ms, and its node id connected again");
        return true;
    }

    /**
     * Whether this connection holds the node id on the lane: claimed, and neither closed nor closing.
     */
    boolean holdsClaim(String nodeId, int onLane) {
        State current = state;
        return claimed && onLane == lane && nodeId.equals(followerId) && current != State.CLOSED
                && current != State.DROPPED_AS_TOO_SLOW;
    }

    /**
     * Streaming encrypted, the follower having confirmed the keys.
     */
    public boolean isEncrypted() {
        return isConnected() && keyAgreement.getKeys() != null;
    }

    /**
     * This connection's keys once both peers encrypt, null on a clear one.
     */
    @Nullable ConnectionEncryptionKeys getEncryptionKeys() {
        return keyAgreement.getKeys();
    }

    /**
     * @param streamCount the streams the leader replicates, every one of which the follower must hold
     */
    public boolean isLaneInSync(int streamCount) {
        for (int key = 0; key < streamCount; key++) {
            if (carries(key) && !isStreamInSync(key) && streams.get(key).isReplicated()) {
                return false;
            }
        }
        return isConnected();
    }

    /**
     * Whether the stream is replicated over this lane, rather than another of the follower's.
     */
    public boolean carries(int streamKey) {
        return logs.laneOf(streamKey) == lane;
    }

    /**
     * The bytes the leader wrote and has not sent this follower, -1 before it started sending.
     */
    public long getUnsentBytes() {
        long sent = sender.getSentOffset();
        return sent < 0 ? -1 : replicationLog.getTailOffset() - sent;
    }

    @Override
    public void onClose(long epoch, CloseReason reason, String text) {
        log.info("Follower {} closed the replication stream: {} {}", followerId, reason, text);
        if (markClosed()) {
            stats.getCloses().closed(reason);
        }
    }

    /**
     * See {@link org.lolaf.replicos.RegisteredStream#reconfigure}.
     */
    void reannounce(int streamKey) {
        sender.reannounce(streamKey);
    }

    void onWriteWatermark(boolean reached) {
        sender.onWriteWatermark(reached);
    }

    /**
     * For a leader stopping cleanly, its writers already stopped: waits for every record of the log to be sent, then
     * closes with {@link CloseReason#SHUTDOWN}. Arriving after them on the same connection, it tells the follower it
     * holds everything, so the follower takes over knowing it lost no write. Returns once the follower hung up
     * on it, so it has read it before leadership moves. When the deadline passes first, the connection is dropped.
     *
     * @return whether the follower read it
     */
    boolean handOver(Deadline deadline) {
        if (state != State.STREAMING) {
            return false;
        }
        boolean sentAll = deadline.waitAsLongAs(() -> state == State.STREAMING
                && sender.getSentOffset() != replicationLog.getTailOffset()) && state == State.STREAMING;
        sender.stop();
        boolean senderStopped = deadline.waitAsLongAs(sender::isRunning);
        if (!sentAll || !senderStopped) {
            log.warn("Follower {} was not sent every record before the deadline: it will not take over as handed over",
                    followerId);
            if (markClosed()) {
                stats.getCloses().disconnected();
            }
            ioSession.stop(Deadline.immediate());
            return false;
        }
        if (!requestClose(PendingClose.thenAwaitHangUp(CloseReason.SHUTDOWN,
                "the leader stopped cleanly, every record was sent"))) {
            return false;
        }
        boolean read = deadline.waitAsLongAs(() -> !followerHungUp);
        ioSession.stop(Deadline.of(CLOSE_FLUSH));
        if (!read) {
            log.warn("Follower {} did not hang up on the hand over before the deadline: it may not take over as handed over",
                    followerId);
        }
        return read;
    }

    /**
     * Sends a CLOSE and disconnects, the connection flushing it first. Any thread; only the first close of the
     * connection does it. The IO thread sends it once the sender stopped, the sender leaving its loop at its next turn,
     * so it is sealed in order after the sender's last frame.
     *
     * @return false when the connection was already closed, and nothing sent
     */
    boolean close(CloseReason reason, String text) {
        return requestClose(PendingClose.thenStop(reason, text));
    }

    private boolean requestClose(PendingClose closing) {
        // recorded before the mark: a sender leaving its loop on the mark finds it
        if (!PENDING_CLOSE.compareAndSet(this, null, closing)) {
            return false;
        }
        if (!markClosed()) {
            PENDING_CLOSE.compareAndSet(this, closing, null);
            return false;
        }
        stats.getCloses().closed(closing.getReason());
        logClose(closing.getReason(), closing.getText());
        if (sender.isRunning()) {
            sender.wakeup();
        } else {
            sendPendingCloseOnIoThread();
        }
        return true;
    }

    /**
     * Run by the sender leaving its loop.
     */
    void senderStopped() {
        if (pendingClose != null) {
            sendPendingCloseOnIoThread();
        }
    }

    private void sendPendingCloseOnIoThread() {
        ioSession.processTask(this::sendPendingClose, (task, refusedOnceDisconnected) -> {
        });
    }

    private void sendPendingClose() {
        PendingClose closing = pendingClose;
        if (closing == null || !closeSent.compareAndSet(false, true)) {
            return;
        }
        FrameCipher sealer = sender.isStreamStarted() ? sender.getSealer() : FrameCipher.CLEAR;
        ByteBuffer frame = ioSession.borrow(FrameEncoder.bytesWith(FrameType.CLOSE, closing.getText())
                + sealer.getTagBytes());
        int length = FrameEncoder.close(frame.clear(), 0, nodeEpoch.getAsLong(), closing.getReason(), closing.getText());
        ioSession.send(BatchBuffers.written(frame, sealer.seal(frame, 0, length)), true);
        if (closing.isStopAfter()) {
            ioSession.stop(Deadline.of(CLOSE_FLUSH));
        }
    }

    private void logClose(CloseReason reason, String shortText) {
        if (reason == CloseReason.SHUTDOWN || reason == CloseReason.RETRY_LATER) {
            log.info("Closing the replication stream to follower {} lane {}: {} {}", followerId, lane, reason, shortText);
        } else if (reason.isConfigurationError()) {
            log.error("Closing the replication stream to follower {} lane {}: {} {}", followerId, lane, reason, shortText);
        } else {
            log.warn("Closing the replication stream to follower {} lane {}: {} {}", followerId, lane, reason, shortText);
        }
    }

    /**
     * The connection is gone: the send loop ends at its next turn.
     */
    void disconnected() {
        followerHungUp = true;
        if (markClosed()) {
            stats.getCloses().disconnected();
        }
    }

    /**
     * This leader stops: unlike a lost connection, not counted as one.
     */
    void leaderStopped() {
        followerHungUp = true;
        markClosed();
    }

    /**
     * Run by the appending thread, which only marks the connection: closing it would hold that thread, and the log,
     * flushing the CLOSE to a follower that does not read. The sender, or the HELLO before it starts, closes it.
     */
    void droppedAsTooSlow() {
        State current;
        do {
            current = state;
            if (current != State.AWAITING_HELLO && current != State.AWAITING_KEY_CONFIRMATION && current != State.STREAMING) {
                return;
            }
        } while (!STATE.compareAndSet(this, current, State.DROPPED_AS_TOO_SLOW));
        stats.droppedTooSlow(followerId, lane);
        sender.wakeup();
    }

    void closeDropped() {
        close(CloseReason.TOO_SLOW, "a write waited the log full timeout for it");
    }

    /**
     * @return false when another thread closed the connection first
     */
    private boolean markClosed() {
        State current;
        do {
            current = state;
            if (current == State.CLOSED) {
                return false;
            }
        } while (!STATE.compareAndSet(this, current, State.CLOSED));
        ReplicationLog.Reader opened = reader;
        if (opened != null) {
            opened.close();
        }
        return true;
    }

    public enum State {
        AWAITING_HELLO,
        /**
         * Both peers encrypt: nothing streams until the follower's KEY_CONFIRMATION verifies.
         */
        AWAITING_KEY_CONFIRMATION,
        STREAMING,
        /**
         * Dropped as too slow by a write, its CLOSE not sent yet.
         */
        DROPPED_AS_TOO_SLOW,
        CLOSED
    }

    @Value
    private static class PendingClose {
        CloseReason reason;
        String text;
        /**
         * Stop the connection once the CLOSE is sent, rather than wait for the follower to hang up as a hand over does.
         */
        boolean stopAfter;

        static PendingClose thenStop(CloseReason reason, String text) {
            return new PendingClose(reason, FrameEncoder.closeText(text), true);
        }

        static PendingClose thenAwaitHangUp(CloseReason reason, String text) {
            return new PendingClose(reason, FrameEncoder.closeText(text), false);
        }
    }
}
