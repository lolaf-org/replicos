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
package org.lolaf.replicos;

import com.github.valfirst.slf4jtest.TestLoggerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lolaf.ringos.Deadline;
import org.lolaf.replicos.journal.Journal;
import org.lolaf.replicos.journal.JournalCopy;
import org.lolaf.replicos.journal.JournalReplica;
import org.lolaf.replicos.journal.JournalStream;
import org.lolaf.replicos.follower.LeaderConnection;
import org.lolaf.replicos.leader.FollowerConnection;
import org.lolaf.replicos.spi.StreamReplicationMode;
import org.slf4j.event.Level;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

/**
 * A leader {@code a} and two followers, {@code b} and {@code c}, over real connections.
 */
class TestSeveralFollowers {

    private static final String MAIN = "main";
    private static final Duration ACK_TIMEOUT = Duration.ofSeconds(5);

    private final Journal journalA;
    private final Journal journalB;
    private final JournalReplica replicaB;
    private final JournalReplica replicaC;
    private final JournalCopy copyB;
    private final JournalCopy copyC;
    private final CountDownLatch bHeld;
    private final CountDownLatch cHeld;
    private final AtomicLong epoch;
    private final InetSocketAddress addressA;
    private final InetSocketAddress addressB;
    private final ReplicationNode a;
    private final ReplicationNode b;
    private final ReplicationNode c;

    TestSeveralFollowers() {
        this.journalA = new Journal();
        this.journalB = new Journal();
        this.replicaB = new JournalReplica();
        this.replicaC = new JournalReplica();
        this.copyB = replicaB.configure(MAIN);
        this.copyC = replicaC.configure(MAIN);
        this.bHeld = new CountDownLatch(1);
        this.cHeld = new CountDownLatch(1);
        this.epoch = new AtomicLong(1);
        this.addressA = new InetSocketAddress("localhost", TestingPorts.findFree());
        this.addressB = new InetSocketAddress("localhost", TestingPorts.findFree());
        this.a = node("a", addressA, journalA, new JournalReplica());
        this.b = node("b", addressB, journalB, replicaB);
        this.c = node("c", new InetSocketAddress("localhost", TestingPorts.findFree()), new Journal(), replicaC);
    }

    private static ReplicationNode node(String nodeId, InetSocketAddress bindAddress, Journal journal,
                                        JournalReplica replica) {
        return node(nodeId, bindAddress, journal, replica, Duration.ZERO, Set.of());
    }

    private static ReplicationNode node(String nodeId, InetSocketAddress bindAddress, Journal journal,
                                        JournalReplica replica, Duration followerStallTimeout,
                                        Set<String> syncFollowers) {
        ReplicationNode node = ReplicationNode.builder()
                .nodeId(nodeId)
                .leader(LeaderSettings.builder()
                        .bindAddress(bindAddress)
                        .streams(journal)
                        .heartbeatInterval(Duration.ofMillis(100))
                        .logFullTimeout(Duration.ofMillis(200))
                        .followerStallTimeout(followerStallTimeout)
                        .syncFollowers(syncFollowers)
                        .build())
                .follower(FollowerSettings.builder()
                        .sinks(replica)
                        .reconnectInterval(Duration.ofMillis(50))
                        .checkInterval(Duration.ofMillis(50))
                        .build())
                .logCapacity(1 << 16)
                .build();
        journal.writeThrough(node);
        return node;
    }

    /**
     * Runs {@code test} with a leader {@code d} of its own settings, which {@code b} and {@code c} follow.
     */
    private void withLeaderD(Duration followerStallTimeout, Set<String> syncFollowers, LeaderDTest test) {
        Journal journalD = new Journal();
        InetSocketAddress addressD = new InetSocketAddress("localhost", TestingPorts.findFree());
        ReplicationNode d = node("d", addressD, journalD, new JournalReplica(), followerStallTimeout, syncFollowers);
        try {
            JournalStream main = journalD.add(MAIN, StreamReplicationMode.SYNC_APPLIED);
            d.lead(1, stop());
            b.follow(addressD, epoch::get, stop());
            c.follow(addressD, epoch::get, stop());
            test.run(d, main);
        } finally {
            bHeld.countDown();
            cHeld.countDown();
            d.close(stop());
        }
    }

    @FunctionalInterface
    private interface LeaderDTest {
        void run(ReplicationNode d, JournalStream main);
    }

    private static Deadline stop() {
        return Deadline.of(Duration.ofSeconds(2));
    }

    private static long errorsMentioning(Class<?> logger, String text) {
        return TestLoggerFactory.getTestLogger(logger).getAllLoggingEvents().stream()
                .filter(event -> event.getLevel() == Level.ERROR && event.getFormattedMessage().contains(text))
                .count();
    }

    @AfterEach
    void close() {
        bHeld.countDown();
        cHeld.countDown();
        TestLoggerFactory.clearAll();
        a.close(stop());
        b.close(stop());
        c.close(stop());
    }

    private JournalStream aLeadsBothFollowers(StreamReplicationMode mode) {
        JournalStream main = journalA.add(MAIN, mode);
        main.append("first");
        a.lead(1, stop());
        b.follow(List.of(addressA, addressB), epoch::get, stop());
        c.follow(List.of(addressA, addressB), epoch::get, stop());
        return main;
    }

    private JournalStream aLeadsBothInSync() {
        JournalStream main = aLeadsBothFollowers(StreamReplicationMode.SYNC_APPLIED);
        await().until(() -> a.followers().countInSync() == 2);
        return main;
    }

    @Test
    void everyFollowerGetsEveryRecordAndTheWaitIsForBoth() {
        JournalStream main = aLeadsBothInSync();

        main.append("second");

        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(copyB.getStored()).containsExactly("1 first", "2 second");
        assertThat(copyC.getStored()).as("acked by both").containsExactly("1 first", "2 second");
        assertThat(a.followers().countConnected()).isEqualTo(2);
        assertThat(a.followers().getSnapshots()).isEqualTo(2);
        assertThat(a.followers().getLaneStatuses()).extracting(FollowerLaneStatus::getNodeId)
                .containsExactlyInAnyOrder("b", "c");
    }

    @Test
    void eachFollowerShowsTheLastRecordItWroteAndTheLeaderTheOffsetEachApplied() {
        JournalStream main = aLeadsBothInSync();

        long offset = main.append("second");

        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(b.leader().getLanePositions()).singleElement().satisfies(position -> {
            assertThat(position.getEpoch()).isEqualTo(1);
            assertThat(position.getLeadership()).isNotZero();
            assertThat(position.getOffset()).isEqualTo(offset);
        });
        assertThat(c.leader().getLanePositions()).as("the same leadership's").isEqualTo(b.leader().getLanePositions());
        assertThat(a.followers().getLaneStatuses()).extracting(FollowerLaneStatus::getAppliedOffset)
                .containsExactly(offset, offset);
    }

        @Test
    void aModeSwitchedOnTheLeaderKeepsEveryFollowersCopy() {
        JournalStream main = aLeadsBothFollowers(StreamReplicationMode.ASYNC);
        await().until(() -> a.followers().countInSync() == 2);

        main.setStreamReplicationMode(StreamReplicationMode.SYNC_APPLIED);
        a.reannounce(0);
        await().until(() -> b.leader().getStream(MAIN).getLeaderMode() == StreamReplicationMode.SYNC_APPLIED
                && c.leader().getStream(MAIN).getLeaderMode() == StreamReplicationMode.SYNC_APPLIED
                && a.followers().countInSync() == 2);
        main.append("second");

        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(copyB.getStored()).containsExactly("1 first", "2 second");
        assertThat(copyC.getStored()).containsExactly("1 first", "2 second");
        assertThat(a.followers().getSnapshots()).as("one each, to join").isEqualTo(2);
    }

    /**
     * The follower's sink write holds its IO thread: closing it used to take 30 seconds whatever the deadline, so the
     * leader's wait timed out on it.
     */
    @Test
    void aFollowerLeavingDuringAWaitLeavesItToTheOther() {
        JournalStream main = aLeadsBothInSync();
        copyC.setWritesHeld(cHeld);
        main.append("second");

        CompletableFuture<Void> leaving = CompletableFuture.runAsync(() -> c.close(stop()));

        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(leaving).succeedsWithin(ACK_TIMEOUT);
        await().until(() -> a.followers().countConnected() == 1);
    }

    @Test
    void aQuorumOfOneIsNotHeldUpByAStuckFollower() {
        JournalStream main = aLeadsBothInSync();
        copyC.setWritesHeld(cHeld);

        main.append("second");

        assertThat(main.awaitAck(1, ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(a.followers().getAckTimeouts(0)).isZero();
        assertThat(copyB.getStored()).containsExactly("1 first", "2 second");
        assertThat(copyC.getStored()).containsExactly("1 first");
    }

    @Test
    void aQuorumAboveTheFollowersLeftEndsTheWaitAtOnceAndIsCounted() {
        JournalStream main = aLeadsBothInSync();
        c.close(stop());
        await().until(() -> a.followers().countConnected() == 1);

        main.append("second");

        assertThat(main.awaitAck(2, ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.NOT_ENOUGH_FOLLOWERS);
        assertThat(main.awaitAck(1, ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(a.followers().getAcksWithNotEnoughFollowers(0)).isEqualTo(1);
    }

    @Test
    void aHeldUpFollowerTimesOneWaitOutThenTheOthersAckIsEnough() {
        JournalStream main = aLeadsBothInSync();
        copyC.setWritesHeld(cHeld);

        main.append("second");
        assertThat(main.awaitAck(Duration.ofMillis(200))).isEqualTo(StreamWriter.Ack.TIMED_OUT);
        main.append("third");

        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(a.followers().getAckTimeouts(0)).isEqualTo(1);
        assertThat(copyB.getStored()).containsExactly("1 first", "2 second", "3 third");
        assertThat(copyC.getStored()).containsExactly("1 first");
        cHeld.countDown();
        await().until(() -> copyC.getStored().size() == 3);
    }

    @Test
    void aFollowerTooSlowIsDroppedWhileTheOtherKeepsUp() {
        JournalStream main = aLeadsBothFollowers(StreamReplicationMode.ASYNC);
        await().until(() -> a.followers().countInSync() == 2);
        copyC.setWritesHeld(cHeld);
        String record = "x".repeat(1000);

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            for (int i = 0; i < 100; i++) {
                main.append(record);
            }
            return a.followers().getDroppedTooSlow() > 0;
        });

        assertThat(a.followers().getDroppedTooSlow()).isEqualTo(1);
        assertThat(a.followers().getDroppedFollowers()).extracting(DroppedFollower::getNodeId).containsExactly("c");
        main.append("last");
        await().until(() -> copyB.getStored().get(copyB.getStored().size() - 1).endsWith(" last"));
        cHeld.countDown();
        await().atMost(Duration.ofSeconds(30)).until(() -> copyC.getStored().equals(copyB.getStored()));
    }

    @Test
    void aFollowerStuckDuringAWaitIsDroppedAsStalledAndTheOtherEndsIt() {
        withLeaderD(Duration.ofSeconds(1), Set.of(), (d, main) -> {
            await().until(() -> d.followers().countInSync() == 2);
            copyC.setWritesHeld(cHeld);

            main.append("first");

            assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
            assertThat(d.followers().getCloses()).isEqualTo(Map.of("STALLED", 1L));
            assertThat(d.followers().getAckTimeouts(0)).isZero();
            assertThat(copyB.getStored()).containsExactly("1 first");
            assertThat(copyC.getStored()).isEmpty();
        });
    }

    @Test
    void aFollowerNotASyncFollowerGetsEveryRecordButNeverAcksASyncAppend() {
        withLeaderD(Duration.ZERO, Set.of("b", "d"), (d, main) -> {
            await().until(() -> d.followers().getLaneStatuses().stream().filter(FollowerLaneStatus::isInSync).count() == 2);
            assertThat(d.followers().getLaneStatuses())
                    .extracting(FollowerLaneStatus::getNodeId, FollowerLaneStatus::isSyncFollower)
                    .containsExactlyInAnyOrder(tuple("b", true), tuple("c", false));
            assertThat(d.followers().countInSync()).isEqualTo(1);
            copyB.setWritesHeld(bHeld);

            main.append("first");

            assertThat(main.awaitAck(1, Duration.ofMillis(200))).as("c's ack does not count").isEqualTo(StreamWriter.Ack.TIMED_OUT);
            await().until(() -> copyC.getStored().equals(List.of("1 first")));
            bHeld.countDown();
            b.close(stop());
            await().until(() -> d.followers().countConnected() == 1);
            main.append("second");
            assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.NO_FOLLOWER_IN_SYNC);
            assertThat(d.followers().isStreamInSync(0)).isFalse();
            await().until(() -> copyC.getStored().equals(List.of("1 first", "2 second")));
        });
    }

    @Test
    void aSecondNodeGivenAConnectedFollowersIdIsRefusedUntilTheFirstLeaves() {
        aLeadsBothInSync();
        ReplicationNode impostor = node("c", new InetSocketAddress("localhost", TestingPorts.findFree()), new Journal(),
                new JournalReplica());
        try {
            impostor.follow(addressA, epoch::get, stop());

            await().until(() -> a.followers().getCloses().containsKey("DUPLICATE_NODE_ID")
                    && impostor.leader().getCloses().containsKey("DUPLICATE_NODE_ID"));
            assertThat(a.followers().countConnected()).isEqualTo(2);
            assertThat(errorsMentioning(FollowerConnection.class, "DUPLICATE_NODE_ID")).as("on the leader").isPositive();
            assertThat(errorsMentioning(LeaderConnection.class, "DUPLICATE_NODE_ID")).as("on the follower").isPositive();
            c.close(stop());
            await().until(() -> impostor.leader().isConnected() && a.followers().countConnected() == 2);
        } finally {
            impostor.close(stop());
        }
    }

    @Test
    void aFollowerPromotedLeadsTheOther() {
        JournalStream main = aLeadsBothInSync();
        main.append("second");
        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);

        assertThat(a.handOver(stop())).isTrue();
        await().until(() -> b.leader().getStream(MAIN).isHandedOver() && c.leader().getStream(MAIN).isHandedOver());
        a.standDown(stop());
        JournalStream promoted = journalB.add(MAIN, StreamReplicationMode.SYNC_APPLIED);
        copyB.getStored().forEach(entry -> promoted.append(entry.substring(entry.indexOf(' ') + 1)));
        epoch.set(2);
        b.lead(2, stop());
        await().until(() -> b.followers().isStreamInSync(0));
        promoted.append("third");

        assertThat(promoted.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(copyC.getStored()).containsExactly("1 first", "2 second", "3 third");
        assertThat(c.leader().getStatus()).isNotNull()
                .satisfies(status -> assertThat(status.getLeaderId()).isEqualTo("b"));
    }
}
