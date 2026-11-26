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
package org.lolaf.replicos.wire;

import org.lolaf.replicos.spi.WireFormatException;
import org.lolaf.replicos.EncryptionMode;
import org.jspecify.annotations.Nullable;
import org.lolaf.replicos.spi.RecordFlag;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.nio.ByteBuffer;

/**
 * Receives decoded frames as primitives, so the frequent ones allocate nothing. A payload or a state is handed over
 * as a range of the receive buffer, valid only for the duration of the call.
 *
 * <p>Every method gets the sender's epoch: fencing a deposed leader applies to every frame alike. A frame the handler
 * does not take, one of the other direction's, is refused: by default every method throws {@link WireFormatException}.
 */
public interface FrameHandler {

    /**
     * A follower's first frame.
     *
     * @param epoch                 the one the follower knows, at most the leader's: a newer one means the leader was
     *                              deposed
     * @param version               the follower's protocol version, which must be the leader's
     * @param applicationId         what the follower replicates, which must be the leader's: two applications' streams
     *                              do not mix
     * @param nodeId                the follower's, naming it in the leader's views, logs and metrics
     * @param lastAppliedEpoch      the epoch the last record it applied came from
     * @param lastAppliedLeadership the leadership of {@code lastAppliedEpoch} the last record applied came from
     * @param lastAppliedOffset     the offset of the last record it applied, -1 for none: the leader resumes after it
     *                              when its log still holds it in the same epoch and leadership
     * @param lane                  which of the leader's lanes this connection carries, 0 for the first, the one that
     *                              learns how many there are
     * @param encryptionMode        the follower's, which with the leader's decides whether the connection encrypts
     * @param nonce                 the follower's random part of this connection's keys, zeros when it does not encrypt
     */
    default void onHello(long epoch, int version, int applicationId, String nodeId, long lastAppliedEpoch,
                         long lastAppliedLeadership, long lastAppliedOffset, int lane, EncryptionMode encryptionMode,
                         byte[] nonce) {
        throw unexpected(FrameType.HELLO);
    }

    /**
     * @param epoch          the leader's, which every later frame of the connection must carry
     * @param version        the leader's protocol version
     * @param applicationId  the leader's, as in {@link #onHello}
     * @param leaderId       the leader's node id, which every lane of the follower must find the same
     * @param leadership     drawn at random each time the leader starts leading: its log's offsets mean nothing to
     *                       another leadership, even of the same epoch
     * @param resumeOffset   where the stream resumes, or {@link WireFormat#NO_RESUME} when every stream is caught up
     *                       by snapshot
     * @param lanes          how many lanes the leader splits its streams over; the follower opens the others
     * @param encryptionMode the leader's, as in {@link #onHello}
     * @param nonce          the leader's, as in {@link #onHello}
     */
    default void onWelcome(long epoch, int version, int applicationId, String leaderId, long leadership,
                           long resumeOffset, int lanes, EncryptionMode encryptionMode, byte[] nonce) {
        throw unexpected(FrameType.WELCOME);
    }

    /**
     * The follower holds the same secret and cipher: {@code confirmation} is the HMAC of the HELLO and WELCOME under a
     * key derived from both. The leader streams only once it verifies.
     *
     * @param epoch        the sender's
     * @param confirmation HMAC-SHA256, 32 bytes
     */
    default void onKeyConfirmation(long epoch, byte[] confirmation) {
        throw unexpected(FrameType.KEY_CONFIRMATION);
    }

    /**
     * Binds a stream key, used by every later frame about the stream, to the stream's name.
     *
     * @param epoch           the sender's
     * @param streamKey       given from 0 in registration order and never reused; its lane is the key modulo the lane
     *                        count
     * @param replicationMode the leader's for the stream, which the follower's configuration must give it too
     * @param streamName      the application's name for the stream, which the follower finds its own copy by
     * @param storageIdentity where the leader stores the stream, null when unknown: a follower storing its copy there
     *                        refuses it
     */
    default void onStream(long epoch, int streamKey, StreamReplicationMode replicationMode, String streamName,
                          @Nullable String storageIdentity) {
        throw unexpected(FrameType.STREAM);
    }

    /**
     * Every stream the leader replicated when the connection started has been announced: until then, the follower
     * cannot tell a stream it lacks from one not announced yet.
     *
     * @param epoch          the sender's
     * @param announcedCount the STREAM frames sent before it on the connection, which the follower checks
     */
    default void onStreamsAnnounced(long epoch, int announcedCount) {
        throw unexpected(FrameType.STREAMS_ANNOUNCED);
    }

    /**
     * The follower's answer to a {@code STREAM}, and again whenever the stream's state on the follower changes, as
     * when a snapshot completes it.
     *
     * @param epoch        the sender's
     * @param streamKey    as the STREAM answered gave it
     * @param answer       whether the follower replicates the stream, and holds it whole
     * @param check        how the follower proves which part of the stream it holds; another than the leader's gets
     *                     the whole stream
     * @param lastPosition the last position the follower holds of the stream, -1 for none
     * @param buffer       the receive buffer, holding the proof at {@code proofIndex}
     * @param proofIndex   where {@code check}'s proof of the follower's copy starts in {@code buffer}
     * @param proofLength  0 for no proof
     */
    default void onStreamAnswer(long epoch, int streamKey, StreamAnswer answer, CatchUpCheck check, long lastPosition,
                                ByteBuffer buffer, int proofIndex, int proofLength) {
        throw unexpected(FrameType.STREAM_ANSWER);
    }

    /**
     * @param epoch         the sender's
     * @param logOffset     the record's position in the leader's replication log
     * @param streamKey     as a STREAM gave it
     * @param contentType   the application's, telling what the payload holds
     * @param flags         {@link WireFormat#MORE_FRAGMENTS}, {@link WireFormat#CONTINUED_FRAGMENT} and
     *                      {@link RecordFlag#RESET}; zero for a whole record
     * @param position      the record's place in its stream, increasing between two resets
     * @param buffer        the receive buffer, holding the payload or fragment at {@code payloadIndex}
     * @param payloadIndex  where the payload starts in {@code buffer}
     * @param payloadLength the payload's, or this fragment's
     */
    default void onRecord(long epoch, long logOffset, int streamKey, byte contentType, byte flags, long position,
                          ByteBuffer buffer, int payloadIndex, int payloadLength) {
        throw unexpected(FrameType.RECORD);
    }

    /**
     * @param epoch          the sender's
     * @param streamKey      as a STREAM gave it
     * @param snapshotOffset the log offset the snapshot starts at; the stream's records after it come through the
     *                       snapshot until its end, and through the log again after that
     * @param fromPosition   the first position the snapshot copies: the follower keeps what it holds before it
     * @param buffer         the receive buffer, holding the stream's state at {@code stateIndex}
     * @param stateIndex     where the stream's state starts in {@code buffer}, the application's to resume from
     * @param stateLength    0 for no state
     */
    default void onSnapshotBegin(long epoch, int streamKey, long snapshotOffset, long fromPosition, ByteBuffer buffer,
                                 int stateIndex, int stateLength) {
        throw unexpected(FrameType.SNAPSHOT_BEGIN);
    }

    /**
     * As {@link #onRecord}: a snapshot carries a payload longer than a frame as consecutive fragments too.
     *
     * @param epoch         the sender's
     * @param streamKey     the stream the snapshot in progress copies
     * @param contentType   as for {@link #onRecord}
     * @param flags         as for {@link #onRecord}, {@link RecordFlag#RESET} aside
     * @param position      the record's place in its stream, increasing through the snapshot
     * @param buffer        the receive buffer, holding the payload or fragment at {@code payloadIndex}
     * @param payloadIndex  where the payload starts in {@code buffer}
     * @param payloadLength the payload's, or this fragment's
     */
    default void onSnapshotRecord(long epoch, int streamKey, byte contentType, byte flags, long position,
                                  ByteBuffer buffer, int payloadIndex, int payloadLength) {
        throw unexpected(FrameType.SNAPSHOT_RECORD);
    }

    /**
     * @param epoch       the sender's
     * @param streamKey   the stream the snapshot copied
     * @param recordCount the records the snapshot carried, a fragmented one counted once: the follower checks it
     *                    received them all
     * @param buffer      the receive buffer, holding the state at {@code stateIndex}
     * @param stateIndex  as for {@link #onSnapshotBegin}: the stream's state when the snapshot caught up with it,
     *                    which supersedes the one it began with
     * @param stateLength 0 for no state
     */
    default void onSnapshotEnd(long epoch, int streamKey, int recordCount, ByteBuffer buffer, int stateIndex,
                               int stateLength) {
        throw unexpected(FrameType.SNAPSHOT_END);
    }

    /**
     * @param epoch         the sender's
     * @param appliedOffset the offset of the last record the follower applied, which releases the {@code SYNC} sends
     *                      waiting for it
     * @param inSync        whether the follower holds every stream the leader replicates
     * @param pendingWrites what the follower's sinks for this lane have yet to write, as of the last second or so
     */
    default void onAck(long epoch, long appliedOffset, boolean inSync, long pendingWrites) {
        throw unexpected(FrameType.ACK);
    }

    /**
     * Sent before a read's records are written to the follower's sinks, then followed by its {@link #onAck ACK}.
     *
     * @param epoch          the sender's
     * @param receivedOffset the offset of the last record the follower holds in memory, which releases the
     *                       {@code SYNC_RECEIVED} sends waiting for it
     */
    default void onReceived(long epoch, long receivedOffset) {
        throw unexpected(FrameType.RECEIVED);
    }

    /**
     * Sent when the leader sent nothing for a heartbeat interval. The follower answers with an ACK, so a quiet stream
     * still carries traffic both ways.
     *
     * @param epoch         the sender's
     * @param logTailOffset where the leader's log ends
     */
    default void onHeartbeat(long epoch, long logTailOffset) {
        throw unexpected(FrameType.HEARTBEAT);
    }

    /**
     * The sender's last frame before it disconnects. A {@link CloseReason#SHUTDOWN} from the leader tells the follower
     * it was sent every record, so it takes over knowing it lost no write.
     *
     * @param epoch  the sender's
     * @param reason why the sender disconnects
     * @param text   the detail for a person reading the logs, empty for none
     */
    default void onClose(long epoch, CloseReason reason, String text) {
        throw unexpected(FrameType.CLOSE);
    }

    private static WireFormatException unexpected(FrameType type) {
        return new WireFormatException("Unexpected " + type + " frame");
    }
}
