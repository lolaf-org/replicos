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
package org.lolaf.replicos.follower;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.lolaf.ringos.Deadline;
import org.lolaf.replicos.ReplicaStream;
import org.lolaf.replicos.spi.RecordFlag;
import org.lolaf.replicos.spi.ReplicaSink;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.spi.StreamReplicationMode;
import org.lolaf.replicos.wire.RecordDigest;
import org.lolaf.replicos.wire.WireFormat;

import java.nio.ByteBuffer;

/**
 * One of the leader's streams as this follower holds it: its sink, none when it is not replicated here, and how far a
 * snapshot and the log brought it.
 */
@Slf4j
final class BoundStream implements ReplicaStream {

    private static final SinkMethod APPLY = ReplicaSink::apply;
    private static final SinkMethod SNAPSHOT_RECORD = (sink, contentType, flag, position, buffer, index, length) ->
            sink.snapshotRecord(contentType, position, buffer, index, length);

    @Getter
    private final String streamName;
    @Getter
    private final @Nullable ReplicaSink sink;
    @Getter
    private final StreamAnswer notReplicatedAnswer;
    /**
     * Kept by a stream refused until this node's configuration changes, to bind it again once it does.
     */
    @Getter(AccessLevel.PACKAGE)
    private final @Nullable String leaderStorageIdentity;
    /**
     * The mode the leader gives the stream: whether its records are acknowledged on receipt, and kept by a refused
     * stream to bind it again.
     */
    @Getter
    @Setter(AccessLevel.PACKAGE)
    private volatile StreamReplicationMode leaderMode;
    @Getter
    private volatile boolean inSync;
    @Getter
    private volatile boolean handedOver;
    private int snapshotRecordCount;
    /**
     * The last position the follower told the leader it holds, past which no snapshot may start; -1 for none.
     */
    private long claimedLastPosition;
    private boolean assembling;
    private long assembledPosition;
    private byte assembledContentType;
    private RecordFlag assembledFlag;
    /**
     * The fragments received so far of a payload longer than a frame, grown as needed and kept for the next one.
     */
    private @Nullable ByteBuffer assembledPayload;

    private BoundStream(String streamName, StreamReplicationMode leaderMode, @Nullable ReplicaSink sink,
                        StreamAnswer notReplicatedAnswer, @Nullable String leaderStorageIdentity) {
        this.streamName = streamName;
        this.leaderMode = leaderMode;
        this.sink = sink;
        this.notReplicatedAnswer = notReplicatedAnswer;
        this.leaderStorageIdentity = leaderStorageIdentity;
        this.claimedLastPosition = -1;
        this.assembledFlag = RecordFlag.NONE;
    }

    static BoundStream replicated(String streamName, StreamReplicationMode leaderMode, ReplicaSink sink) {
        return new BoundStream(streamName, leaderMode, sink, StreamAnswer.SNAPSHOT_NEEDED, null);
    }

    /**
     * @param leaderStorageIdentity kept to bind the stream again, for an answer this node's configuration may change
     */
    static BoundStream notReplicated(String streamName, StreamReplicationMode leaderMode, StreamAnswer answer,
                                     @Nullable String leaderStorageIdentity) {
        return new BoundStream(streamName, leaderMode, null, answer, leaderStorageIdentity);
    }

    public boolean isReplicated() {
        return sink != null;
    }

    StreamAnswer answer() {
        if (sink == null) {
            return notReplicatedAnswer;
        }
        return inSync ? StreamAnswer.REPLICATING : StreamAnswer.SNAPSHOT_NEEDED;
    }

    boolean isOutOfSync() {
        return sink != null ? !inSync : notReplicatedAnswer.isRefusal();
    }

    void handOver() {
        handedOver = sink != null && inSync;
    }

    long getPendingWrites() {
        return sink == null ? 0 : sink.getPendingWrites();
    }

    boolean isRefusedUntilConfigurationChange() {
        return sink == null && notReplicatedAnswer == StreamAnswer.UNKNOWN_STREAM;
    }

    void streamStarted(boolean resumed) {
        handedOver = false;
        // a resume may start past the first fragments of a payload this stream was assembling
        inSync &= resumed && !assembling;
        discardAssembledPayload();
        discardPending();
    }

    /**
     * Hands the sink nothing while the stream waits for a snapshot, which would discard it anyway: stored past the gap
     * the snapshot fills, a record would let {@link #claim} prove a copy missing that gap.
     *
     * @return false for a fragment out of place
     */
    boolean apply(byte contentType, RecordFlag flag, long position, ByteBuffer receiveBuffer, int payloadIndex,
                  int payloadLength, byte fragmentFlags) {
        @Nullable ReplicaSink target = inSync ? sink : null;
        if (fragmentFlags != 0 || assembling) {
            return assemble(target, contentType, flag, position, receiveBuffer, payloadIndex, payloadLength,
                    fragmentFlags, APPLY);
        }
        if (target != null) {
            target.apply(contentType, flag, position, receiveBuffer, payloadIndex, payloadLength);
        }
        return true;
    }

    /**
     * Digests the sink's last records, which a snapshot may then start after.
     *
     * @return the last position digested, -1 for none, as when the sink fails to read them back
     */
    long claim(int records, RecordDigest digest) {
        digest.reset();
        try {
            sink.readLast(records, digest);
        } catch (RuntimeException e) {
            log.warn("Stream {} could not read its last records back, caught up by a full copy", streamName, e);
            digest.reset();
        }
        claimedLastPosition = digest.getLastPosition();
        return claimedLastPosition;
    }

    void unclaim() {
        claimedLastPosition = -1;
    }

    /**
     * Only for a stream replicated here.
     *
     * @return false for a snapshot starting past what the follower claimed it holds
     */
    boolean snapshotBegin(long fromPosition, ByteBuffer state, int stateIndex, int stateLength) {
        if (fromPosition > claimedLastPosition + 1) {
            return false;
        }
        inSync = false;
        discardAssembledPayload();
        discardPending();
        snapshotRecordCount = 0;
        sink.snapshotBegin(fromPosition, state, stateIndex, stateLength);
        return true;
    }

    /**
     * @return false for a fragment out of place
     */
    boolean snapshotRecord(byte contentType, long position, ByteBuffer receiveBuffer, int payloadIndex,
                           int payloadLength, byte fragmentFlags) {
        if (fragmentFlags != 0 || assembling) {
            if (!assemble(sink, contentType, RecordFlag.NONE, position, receiveBuffer, payloadIndex, payloadLength,
                    fragmentFlags, SNAPSHOT_RECORD)) {
                return false;
            }
        } else {
            sink.snapshotRecord(contentType, position, receiveBuffer, payloadIndex, payloadLength);
        }
        if ((fragmentFlags & WireFormat.MORE_FRAGMENTS) == 0) {
            snapshotRecordCount++;
        }
        return true;
    }

    /**
     * Brings the stream in sync with the leader's state at the snapshot's end: its records in between went through
     * the snapshot.
     *
     * @return false when the count is not what was received
     */
    boolean snapshotEnd(int recordCount, ByteBuffer state, int stateIndex, int stateLength) {
        if (snapshotRecordCount != recordCount) {
            return false;
        }
        sink.writePending();
        sink.snapshotEnd(state, stateIndex, stateLength);
        inSync = true;
        return true;
    }

    void writePending() {
        if (sink != null) {
            sink.writePending();
        }
    }

    void discardPending() {
        if (sink != null) {
            sink.discardPending();
        }
    }

    /**
     * Adds a fragment of a payload longer than a frame, applying the record, with its first fragment's flag, once the
     * last arrives. With no {@code target} it keeps no bytes, only where the payload ends. A continued fragment with no
     * payload begun is what a resume leaves of a record whose start it skipped: dropped, its stream catching up by
     * snapshot.
     *
     * @param assembled the sink method the whole payload goes to
     * @return false for a fragment out of place: another record's, or a new record before the last one ended
     */
    private boolean assemble(@Nullable ReplicaSink target, byte contentType, RecordFlag flag, long position,
                             ByteBuffer receiveBuffer, int fragmentIndex, int fragmentLength, byte fragmentFlags,
                             SinkMethod assembled) {
        boolean continued = (fragmentFlags & WireFormat.CONTINUED_FRAGMENT) != 0;
        boolean moreFragments = (fragmentFlags & WireFormat.MORE_FRAGMENTS) != 0;
        if (continued != assembling) {
            return continued;
        }
        if (!assembling) {
            assembling = true;
            assembledPosition = position;
            assembledContentType = contentType;
            assembledFlag = flag;
        } else if (position != assembledPosition) {
            return false;
        }
        if (target != null) {
            ByteBuffer fragment = receiveBuffer.duplicate();
            fragment.limit(fragmentIndex + fragmentLength).position(fragmentIndex);
            assembledPayload = assembledPayloadWithRoomFor(fragmentLength).put(fragment);
        }
        if (!moreFragments) {
            if (target != null) {
                assembled.call(target, assembledContentType, assembledFlag, position, assembledPayload, 0,
                        assembledPayload.position());
            }
            discardAssembledPayload();
        }
        return true;
    }

    private ByteBuffer assembledPayloadWithRoomFor(int length) {
        if (assembledPayload == null) {
            return ByteBuffer.allocate(length);
        }
        if (assembledPayload.remaining() >= length) {
            return assembledPayload;
        }
        ByteBuffer grown = ByteBuffer.allocate(Math.max(assembledPayload.position() + length,
                assembledPayload.capacity() * 2));
        return grown.put(assembledPayload.flip());
    }

    private void discardAssembledPayload() {
        assembling = false;
        if (assembledPayload != null) {
            assembledPayload.clear();
        }
    }

    void stop(Deadline deadline) {
        discardPending();
        if (sink != null) {
            sink.stop(deadline);
        }
    }

    @FunctionalInterface
    private interface SinkMethod {

        void call(ReplicaSink sink, byte contentType, RecordFlag flag, long position, ByteBuffer buffer,
                  int payloadIndex, int payloadLength);
    }
}
