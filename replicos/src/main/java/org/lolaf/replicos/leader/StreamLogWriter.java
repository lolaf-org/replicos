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

import org.jspecify.annotations.Nullable;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.replicos.StreamWriter;
import org.lolaf.replicos.log.LogFullWait;
import org.lolaf.replicos.log.LogRecord;
import org.lolaf.replicos.log.ReplicationLog;
import org.lolaf.replicos.spi.LatencyRecorder;
import org.lolaf.replicos.spi.RecordFlag;
import org.lolaf.replicos.spi.ReplicationClock;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.nio.ByteBuffer;
import java.time.Duration;

/**
 * Appends to the log of the stream's lane, wakes the followers' senders, and waits on {@link Followers} for acks.
 */
public final class StreamLogWriter implements StreamWriter {

    private final int streamKey;
    private final ReplicationLog replicationLog;
    private final Followers followers;
    private final LogRecord logRecord;
    private final LogFullWait logFullWait;
    private final LatencyRecorder logFullWaitLatency;
    private final IdleStrategy ackIdle;
    private final ReplicationStats stats;
    private final ReplicationStats.AckCounts ackCounts;
    private volatile boolean waiterAdded;

    /**
     * @param logFullWaitLatency the lane's, shared with the other writers of its streams
     * @param ackIdle            what an ack wait parks on, woken by the acks
     */
    public StreamLogWriter(int streamKey, ReplicationLog replicationLog, Followers followers, Duration logFullTimeout,
                           LatencyRecorder logFullWaitLatency, IdleStrategy ackIdle, ReplicationStats stats,
                           ReplicationClock clock) {
        this.streamKey = streamKey;
        this.replicationLog = replicationLog;
        this.followers = followers;
        this.logRecord = new LogRecord();
        this.logFullWait = new LogFullWait(logFullTimeout, clock);
        this.logFullWaitLatency = logFullWaitLatency;
        this.ackIdle = ackIdle;
        this.stats = stats;
        this.ackCounts = stats.acks(streamKey);
    }

    @Override
    public long append(byte contentType, RecordFlag flag, long position, @Nullable ByteBuffer buffer, int payloadIndex,
                       int payloadLength) {
        long offset = replicationLog.append(logRecord.set(streamKey, contentType, flag, position, buffer, payloadIndex,
                payloadLength), logFullWait);
        followers.wakeSenders();
        long waitedNanos = logFullWait.takeWaitedNanos();
        if (waitedNanos >= 0) {
            stats.logFullWaited();
            logFullWaitLatency.record(waitedNanos);
        }
        return offset;
    }

    @Override
    public int getMaxPayloadLength() {
        return replicationLog.getMaxPayloadLength();
    }

    @Override
    public Ack awaitAck(long offset, StreamReplicationMode mode, int quorum, long timeoutNanos) {
        if (quorum < 0) {
            throw new IllegalArgumentException("A quorum of " + quorum + " followers");
        }
        if (!waiterAdded) {
            followers.addWaiter(ackIdle);
            waiterAdded = true;
        }
        Ack ack = followers.awaitAck(streamKey, offset, mode, quorum, timeoutNanos, ackIdle);
        ackCounts.count(ack);
        return ack;
    }

    @Override
    public void close() {
        if (waiterAdded) {
            followers.removeWaiter(ackIdle);
            waiterAdded = false;
        }
    }
}
