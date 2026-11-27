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

import org.lolaf.replicos.CatchUp;
import org.lolaf.replicos.LeaderSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.replicos.spi.ReplicationMetrics;
import org.lolaf.replicos.StreamWriter;
import org.lolaf.replicos.EncryptionMode;
import org.lolaf.replicos.testkit.TestingKeyExchanges;
import org.lolaf.replicos.journal.Journal;
import org.lolaf.replicos.journal.JournalStream;
import org.lolaf.replicos.log.ReplicationLogs;
import org.lolaf.replicos.spi.StreamReplicationMode;
import org.lolaf.replicos.testkit.AllocatedBytes;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.wire.WireFormat;
import org.lolaf.replicos.spi.TestingReplicationClock;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Isolated
class TestFollowers {

    private static final long EPOCH = 7;
    private static final long LEADERSHIP = 41;
    private static final long NO_TIMEOUT = 0;
    private static final int KEY = 0;
    private static final int ALL = StreamWriter.ALL_FOLLOWERS;

    private final Journal journal;
    private final Followers followers;
    private final IdleStrategy idle;
    private final long offset;

    TestFollowers() {
        this.journal = Journal.ofCapacity(1 << 16);
        this.followers = new Followers(new TestingReplicationClock());
        this.idle = new BackoffIdleStrategy();
        JournalStream main = journal.add("main", StreamReplicationMode.SYNC_APPLIED);
        main.append("first");
        offset = journal.getLogs().get(0).getTailOffset();
        main.append("second");
    }

    private static Journal twoLanes() {
        Journal twoLanes = new Journal(new ReplicationLogs(2, 1 << 16), Duration.ofMillis(50));
        twoLanes.add("main", StreamReplicationMode.SYNC_APPLIED);
        twoLanes.add("other", StreamReplicationMode.SYNC_APPLIED);
        return twoLanes;
    }

    private FollowerConnection inSyncFollower(long appliedOffset) {
        return inSyncFollower("follower", appliedOffset);
    }

    private FollowerConnection inSyncFollower(String followerId, long appliedOffset) {
        return inSyncFollower(journal.settings().build(), followerId, appliedOffset);
    }

    private FollowerConnection inSyncFollower(LeaderSettings settings, String followerId, long appliedOffset) {
        FollowerConnection connection = hello(settings, followerId);
        connection.getSender().fillBatch(ByteBuffer.allocate(1 << 16));
        FollowerConnectionHarness.answer(connection, KEY, StreamAnswer.REPLICATING);
        connection.onAck(EPOCH, appliedOffset, true, 0);
        return connection;
    }

    private FollowerConnection hello(LeaderSettings settings, String followerId) {
        IOSession ioSession = mock(IOSession.class);
        when(ioSession.borrow(anyInt())).then(invocation -> ByteBuffer.allocate(invocation.getArgument(0)));
        FollowerConnection connection = new FollowerConnection(ioSession, settings, journal.getLogs(),
                () -> EPOCH, LEADERSHIP, "leader-a", CatchUp.fullCopy(), new ReplicationStats(new SlowFollowers(Duration.ZERO, new TestingReplicationClock()), ReplicationMetrics.NONE), loop -> {
        }, new TestingReplicationClock(), TestingKeyExchanges.CLEAR, followers);
        followers.add(connection);
        connection.onHello(EPOCH, WireFormat.VERSION, Journal.APPLICATION_ID, followerId, EPOCH, LEADERSHIP, 0, 0, EncryptionMode.DISABLED, TestingKeyExchanges.NO_NONCE);
        return connection;
    }

    @Test
    void withoutAFollowerInSyncTheWaitEndsAtOnceEvenWithoutATimeout() {
        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.NO_FOLLOWER_IN_SYNC);

        FollowerConnectionHarness.answer(inSyncFollower(0), KEY, StreamAnswer.SNAPSHOT_NEEDED);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.NO_FOLLOWER_IN_SYNC);
    }

    @Test
    void aRecordTheFollowerAppliedNeedsNoWait() {
        inSyncFollower(offset);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void anAckLateByMoreThanTheTimeoutEndsTheWait() {
        inSyncFollower(0);

        long start = System.nanoTime();
        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, TimeUnit.MILLISECONDS.toNanos(20), idle)).isEqualTo(StreamWriter.Ack.TIMED_OUT);
        assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(20));
    }

    @Test
    void aFollowerAWaitTimedOutOnIsNotWaitedForAgainUntilItAppliesWhatItMissed() {
        FollowerConnection follower = inSyncFollower(0);
        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, TimeUnit.MILLISECONDS.toNanos(20), idle)).isEqualTo(StreamWriter.Ack.TIMED_OUT);

        assertThat(CompletableFuture.supplyAsync(() -> followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, new BackoffIdleStrategy())))
                .as("no second wait, even without a timeout").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(StreamWriter.Ack.TIMED_OUT);

        follower.onAck(EPOCH, offset, true, 0);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void theWaitEndsWhenTheFollowerAcks() {
        FollowerConnection follower = inSyncFollower(0);

        CompletableFuture<StreamWriter.Ack> ack = CompletableFuture.supplyAsync(() -> followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, new BackoffIdleStrategy()));
        follower.onAck(EPOCH, offset, true, 0);

        assertThat(ack).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void aSyncReceivedWaitEndsOnceTheFollowerReceivedTheRecord() {
        FollowerConnection follower = inSyncFollower(0);

        follower.onReceived(EPOCH, offset);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_RECEIVED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void aSyncAppliedWaitIsNotEndedByTheRecordReceivedOnly() {
        FollowerConnection follower = inSyncFollower(0);

        follower.onReceived(EPOCH, offset);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, TimeUnit.MILLISECONDS.toNanos(20), idle))
                .isEqualTo(StreamWriter.Ack.TIMED_OUT);
    }

    @Test
    void aSyncReceivedWaitEndsOnAnApplyTheFollowerSentNoReceivedFor() {
        FollowerConnection follower = inSyncFollower(0);

        follower.onAck(EPOCH, offset, true, 0);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_RECEIVED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void anAckWakesAWaitParkedLongerThanTheTest() {
        FollowerConnection follower = inSyncFollower(0);
        long parkNanos = TimeUnit.SECONDS.toNanos(30);
        IdleStrategy parking = new BackoffIdleStrategy(0, 0, parkNanos, parkNanos);
        followers.addWaiter(parking);
        CompletableFuture<StreamWriter.Ack> ack = new CompletableFuture<>();
        Thread waiter = new Thread(() -> ack.complete(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, parking)));
        waiter.start();
        await().until(() -> waiter.getState() == Thread.State.TIMED_WAITING);

        follower.onAck(EPOCH, offset, true, 0);
        followers.wakeWaiters();

        assertThat(ack).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void aFollowerDisconnectingReleasesTheWait() {
        FollowerConnection follower = inSyncFollower(0);

        CompletableFuture<StreamWriter.Ack> ack = CompletableFuture.supplyAsync(() -> followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, new BackoffIdleStrategy()));
        follower.disconnected();

        assertThat(ack).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(StreamWriter.Ack.NO_FOLLOWER_IN_SYNC);
    }

    /**
     * A closed connection stays listed until betty reports it gone, after its CLOSE flush: a reconnection meanwhile
     * would count the follower's writes twice.
     */
    @Test
    void theWritesPendingOnAFollowerAreCountedOnlyOnItsConnectionStreaming() {
        FollowerConnection closed = inSyncFollower(offset);
        closed.onAck(EPOCH, offset, true, 600);
        closed.disconnected();

        FollowerConnection reconnected = inSyncFollower(offset);
        reconnected.onAck(EPOCH, offset, true, 500);

        assertThat(followers.connections()).contains(closed);
        assertThat(followers.getPendingWrites()).isEqualTo(500);
    }

    @Test
    void aSyncWaitIsOnlyForTheLaneCarryingItsStream() {
        Journal twoLanes = twoLanes();
        twoLanes.get(0).append("");
        twoLanes.get(1).append("");
        IOSession ioSession = mock(IOSession.class);
        when(ioSession.borrow(anyInt())).then(invocation -> ByteBuffer.allocate(invocation.getArgument(0)));
        FollowerConnection connectionOfMain = new FollowerConnection(ioSession, twoLanes.settings().build(),
                twoLanes.getLogs(), () -> EPOCH, LEADERSHIP, "leader-a", CatchUp.fullCopy(),
                new ReplicationStats(new SlowFollowers(Duration.ZERO, new TestingReplicationClock()), ReplicationMetrics.NONE), loop -> {
        }, new TestingReplicationClock(), TestingKeyExchanges.CLEAR, followers);
        connectionOfMain.onHello(EPOCH, WireFormat.VERSION, Journal.APPLICATION_ID, "follower", EPOCH, LEADERSHIP, -1, 0, EncryptionMode.DISABLED, TestingKeyExchanges.NO_NONCE);
        connectionOfMain.getSender().fillBatch(ByteBuffer.allocate(1 << 16));
        FollowerConnectionHarness.answer(connectionOfMain, 0, StreamAnswer.REPLICATING);
        connectionOfMain.onAck(EPOCH, twoLanes.getLogs().get(0).getTailOffset(), true, 0);
        followers.add(connectionOfMain);

        assertThat(followers.awaitAck(0, 0, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(followers.awaitAck(1, 0, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, idle)).as("other's lane is not connected")
                .isEqualTo(StreamWriter.Ack.NO_FOLLOWER_IN_SYNC);
    }

    @Test
    void aFollowerIsInSyncOnlyOnceEveryLaneIs() {
        Journal twoLanes = twoLanes();
        FollowerConnection laneOfMain = laneConnection(twoLanes, 0);
        FollowerConnection laneOfOther = laneConnection(twoLanes, 1);
        FollowerConnectionHarness.answer(laneOfMain, 0, StreamAnswer.REPLICATING);

        assertThat(followers.isAnyFollowerInSync(2, 2)).as("other's lane still catching up").isFalse();

        FollowerConnectionHarness.answer(laneOfOther, 1, StreamAnswer.REPLICATING);

        assertThat(followers.isAnyFollowerInSync(2, 2)).isTrue();
        assertThat(followers.countFollowers(2, FollowerConnection::isConnected)).isEqualTo(1);
    }

    private FollowerConnection laneConnection(Journal twoLanes, int lane) {
        IOSession ioSession = mock(IOSession.class);
        when(ioSession.borrow(anyInt())).then(invocation -> ByteBuffer.allocate(invocation.getArgument(0)));
        FollowerConnection connection = new FollowerConnection(ioSession, twoLanes.settings().build(),
                twoLanes.getLogs(), () -> EPOCH, LEADERSHIP, "leader-a", CatchUp.fullCopy(),
                new ReplicationStats(new SlowFollowers(Duration.ZERO, new TestingReplicationClock()), ReplicationMetrics.NONE), loop -> {
        }, new TestingReplicationClock(), TestingKeyExchanges.CLEAR, followers);
        connection.onHello(EPOCH, WireFormat.VERSION, Journal.APPLICATION_ID, "follower", EPOCH, LEADERSHIP, -1, lane, EncryptionMode.DISABLED, TestingKeyExchanges.NO_NONCE);
        connection.getSender().fillBatch(ByteBuffer.allocate(1 << 16));
        followers.add(connection);
        return connection;
    }

    @Test
    void everyFollowerInSyncMustApplyTheRecord() {
        inSyncFollower("b", offset);
        inSyncFollower("c", 0);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, TimeUnit.MILLISECONDS.toNanos(1), idle)).isEqualTo(StreamWriter.Ack.TIMED_OUT);
    }

    @Test
    void theWaitLastsUntilTheLastFollowerInSyncApplies() {
        FollowerConnection b = inSyncFollower("b", 0);
        FollowerConnection c = inSyncFollower("c", 0);
        CompletableFuture<StreamWriter.Ack> ack = CompletableFuture.supplyAsync(() -> followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, new BackoffIdleStrategy()));

        b.onAck(EPOCH, offset, true, 0);

        await().during(Duration.ofMillis(100)).atMost(Duration.ofSeconds(1)).until(() -> !ack.isDone());
        c.onAck(EPOCH, offset, true, 0);
        assertThat(ack).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void aFollowerCatchingUpIsNotWaitedFor() {
        inSyncFollower("b", offset);
        FollowerConnectionHarness.answer(inSyncFollower("c", 0), KEY, StreamAnswer.SNAPSHOT_NEEDED);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void aFollowerATimeoutMadeOverdueIsSkippedWhileTheOtherApplies() {
        long last = journal.getLogs().get(0).getTailOffset();
        FollowerConnection b = inSyncFollower("b", offset);
        FollowerConnection c = inSyncFollower("c", 0);
        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, TimeUnit.MILLISECONDS.toNanos(20), idle)).isEqualTo(StreamWriter.Ack.TIMED_OUT);
        assertThat(b.getAcks().isOverdue()).as("only the laggard").isFalse();
        assertThat(c.getAcks().isOverdue()).isTrue();

        b.onAck(EPOCH, last, true, 0);

        assertThat(followers.awaitAck(KEY, last, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.ACKED);
        c.onAck(EPOCH, offset, true, 0);
        assertThat(followers.awaitAck(KEY, last, StreamReplicationMode.SYNC_APPLIED, ALL, TimeUnit.MILLISECONDS.toNanos(20), idle))
                .as("waited for again once it applied what it missed").isEqualTo(StreamWriter.Ack.TIMED_OUT);
    }

    @Test
    void theLastFollowerToApplyDisconnectingEndsTheWait() {
        inSyncFollower("b", offset);
        FollowerConnection c = inSyncFollower("c", 0);
        CompletableFuture<StreamWriter.Ack> ack = CompletableFuture.supplyAsync(() -> followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, new BackoffIdleStrategy()));

        c.disconnected();

        assertThat(ack).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void aSyncReceivedWaitTakesOneFollowerReceivingAndAnotherApplying() {
        inSyncFollower("b", 0).onReceived(EPOCH, offset);
        inSyncFollower("c", offset);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_RECEIVED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void followersAreCountedEachOnce() {
        inSyncFollower("b", offset);
        inSyncFollower("c", 0);
        FollowerConnectionHarness.answer(inSyncFollower("d", 0), KEY, StreamAnswer.SNAPSHOT_NEEDED);

        assertThat(followers.countFollowers(1, FollowerConnection::isConnected)).isEqualTo(3);
        assertThat(followers.isAnyFollowerInSync(1, 1)).isTrue();
        assertThat(followers.countFollowers(1, connection -> connection.isLaneInSync(1))).as("d catching up").isEqualTo(2);
    }

    @Test
    void aQuorumOfOneEndsTheWaitOnTheFirstFollowerToApply() {
        inSyncFollower("b", 0);
        FollowerConnection c = inSyncFollower("c", offset);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 1, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 2, TimeUnit.MILLISECONDS.toNanos(20), idle))
                .as("two need b too").isEqualTo(StreamWriter.Ack.TIMED_OUT);
        assertThat(c.getAcks().isOverdue()).as("c applied it").isFalse();
    }

    @Test
    void aFollowerSlowerThanTheQuorumIsNeverMarkedOverdue() {
        FollowerConnection b = inSyncFollower("b", 0);
        inSyncFollower("c", offset);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 1, TimeUnit.MILLISECONDS.toNanos(20), idle))
                .isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(b.getAcks().isOverdue()).isFalse();
    }

    @Test
    void aQuorumAboveTheFollowersInSyncEndsTheWaitAtOnce() {
        inSyncFollower("b", offset);
        FollowerConnectionHarness.answer(inSyncFollower("c", offset), KEY, StreamAnswer.SNAPSHOT_NEEDED);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 2, NO_TIMEOUT, idle))
                .as("c catching up does not count").isEqualTo(StreamWriter.Ack.NOT_ENOUGH_FOLLOWERS);
        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, idle))
                .as("all is however many are in sync").isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void aQuorumTheFollowersNotOverdueCannotReachEndsTheWaitAtOnce() {
        FollowerConnection b = inSyncFollower("b", 0);
        inSyncFollower("c", 0);
        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 2, TimeUnit.MILLISECONDS.toNanos(20), idle))
                .isEqualTo(StreamWriter.Ack.TIMED_OUT);
        b.onAck(EPOCH, offset, true, 0);

        assertThat(CompletableFuture.supplyAsync(() -> followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 2, NO_TIMEOUT, new BackoffIdleStrategy())))
                .as("c still overdue").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(StreamWriter.Ack.TIMED_OUT);
        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 1, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.ACKED);
    }

    @Test
    void aTimeoutUnderAQuorumMarksOverdueOnlyTheFollowersThatHaveNotApplied() {
        FollowerConnection b = inSyncFollower("b", 0);
        FollowerConnection c = inSyncFollower("c", 0);
        FollowerConnection d = inSyncFollower("d", offset);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 2, TimeUnit.MILLISECONDS.toNanos(20), idle))
                .isEqualTo(StreamWriter.Ack.TIMED_OUT);
        assertThat(b.getAcks().isOverdue()).isTrue();
        assertThat(c.getAcks().isOverdue()).isTrue();
        assertThat(d.getAcks().isOverdue()).isFalse();
    }

    @Test
    void withoutSyncFollowersEveryFollowerIsOne() {
        assertThat(inSyncFollower("b", offset).isSyncFollower()).isTrue();
    }

    @Test
    void aFollowerNotASyncFollowerIsNeitherWaitedForNorMarkedOverdue() {
        LeaderSettings standbyOnly = journal.settings().syncFollower("standby").build();
        inSyncFollower(standbyOnly, "standby", 0);
        FollowerConnection dr = inSyncFollower(standbyOnly, "dr", offset);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 1, TimeUnit.MILLISECONDS.toNanos(20), idle))
                .as("dr applied it, but does not count").isEqualTo(StreamWriter.Ack.TIMED_OUT);
        assertThat(dr.isSyncFollower()).isFalse();
        assertThat(dr.getAcks().isOverdue()).isFalse();
    }

    @Test
    void withOnlyFollowersNotSyncFollowersNoFollowerIsInSync() {
        LeaderSettings standbyOnly = journal.settings().syncFollower("standby").build();
        inSyncFollower(standbyOnly, "dr", offset);

        assertThat(followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, ALL, NO_TIMEOUT, idle)).isEqualTo(StreamWriter.Ack.NO_FOLLOWER_IN_SYNC);
        assertThat(followers.isAnyFollowerInSync(1, 1)).isFalse();
    }

    @Test
    void aSecondConnectionOfANodeIdConnectedOnTheLaneIsRefusedUntilTheFirstIsGone() {
        FollowerConnection first = inSyncFollower("b", offset);

        FollowerConnection second = hello(journal.settings().build(), "b");

        assertThat(second.getState()).isEqualTo(FollowerConnection.State.CLOSED);
        assertThat(first.isConnected()).isTrue();
        first.disconnected();
        assertThat(hello(journal.settings().build(), "b").getState()).isEqualTo(FollowerConnection.State.STREAMING);
    }

    @Test
    void waitingAllocatesNothing() {
        inSyncFollower("b", offset);
        inSyncFollower("c", 0);
        followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 1, NO_TIMEOUT, idle);

        long before = AllocatedBytes.ofCurrentThread();
        for (int i = 0; i < 100_000; i++) {
            followers.awaitAck(KEY, offset, StreamReplicationMode.SYNC_APPLIED, 1, NO_TIMEOUT, idle);
        }

        assertThat(AllocatedBytes.ofCurrentThread() - before).as("less than a byte per wait").isLessThan(100_000);
    }

    @Test
    void theListOfFollowersIsBuiltOnlyWhenOneJoinsOrLeaves() {
        FollowerConnection first = inSyncFollower("b", offset);
        assertThat(followers.connections()).containsExactly(first).isSameAs(followers.connections());

        FollowerConnection second = inSyncFollower("c", offset);
        assertThat(followers.connections()).containsExactly(first, second);

        followers.remove(first);
        assertThat(followers.connections()).containsExactly(second);
    }
}
