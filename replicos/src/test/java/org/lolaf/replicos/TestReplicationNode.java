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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.lolaf.ringos.Deadline;
import org.lolaf.replicos.journal.Journal;
import org.lolaf.replicos.journal.JournalCopy;
import org.lolaf.replicos.journal.JournalReplica;
import org.lolaf.replicos.journal.JournalStream;
import org.lolaf.replicos.spi.LatencyRecorder;
import org.lolaf.replicos.spi.RecordFlag;
import org.lolaf.replicos.spi.ReplicationMetrics;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.spi.StreamReplicationMode;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Two nodes, {@code a} and {@code b}, over real connections, each replicating its own journal while it leads and
 * holding the other's while it follows.
 */
class TestReplicationNode {

    private static final String MAIN = "main";
    private static final Duration ACK_TIMEOUT = Duration.ofSeconds(5);

    private final Journal journalA;
    private final Journal journalB;
    private final JournalReplica replicaB;
    private final JournalCopy copyB;
    private final InetSocketAddress addressA;
    private final InetSocketAddress addressB;
    private final List<String> recordersAskedFor;
    private final List<String> catchUps;
    private final ReplicationNode a;
    private final ReplicationNode b;

    TestReplicationNode() {
        this.journalA = new Journal();
        this.journalB = new Journal();
        this.replicaB = new JournalReplica();
        this.copyB = replicaB.configure(MAIN, StreamReplicationMode.SYNC_APPLIED);
        this.addressA = new InetSocketAddress("localhost", TestingPorts.findFree());
        this.addressB = new InetSocketAddress("localhost", TestingPorts.findFree());
        this.recordersAskedFor = new CopyOnWriteArrayList<>();
        this.catchUps = new CopyOnWriteArrayList<>();
        this.a = node("a", addressA, journalA, new JournalReplica(), ReplicationEncryptionSettings.builder().build());
        this.b = node("b", addressB, journalB, replicaB, ReplicationEncryptionSettings.builder().build());
    }

    private ReplicationNode node(String nodeId, InetSocketAddress bindAddress, Journal journal, JournalReplica replica,
                                 ReplicationEncryptionSettings encryption) {
        return node(nodeId, bindAddress, journal, replica, encryption, CatchUp.fullCopy());
    }

    private ReplicationNode node(String nodeId, InetSocketAddress bindAddress, Journal journal, JournalReplica replica,
                                 ReplicationEncryptionSettings encryption, CatchUp catchUp) {
        ReplicationNode node = ReplicationNode.builder()
                .nodeId(nodeId)
                .leader(LeaderSettings.builder()
                        .bindAddress(bindAddress)
                        .streams(journal)
                        .heartbeatInterval(Duration.ofMillis(100))
                        .build())
                .follower(FollowerSettings.builder()
                        .sinks(replica)
                        .reconnectInterval(Duration.ofMillis(50))
                        .checkInterval(Duration.ofMillis(50))
                        .build())
                .encryption(encryption)
                .catchUp(catchUp)
                .logCapacity(1 << 16)
                .metrics(new ReplicationMetrics() {
                    @Override
                    public LatencyRecorder followerAckLatency(String followerId, int lane) {
                        recordersAskedFor.add(nodeId + " times " + followerId + " lane " + lane);
                        return LatencyRecorder.NONE;
                    }

                    @Override
                    public LatencyRecorder followerCatchUp(String followerId, int lane) {
                        return durationInNanos -> catchUps.add(nodeId + " caught up " + followerId + " lane " + lane
                                + (durationInNanos > 0 ? " after a while" : " at once"));
                    }

                    @Override
                    public LatencyRecorder logFullWait(int lane) {
                        recordersAskedFor.add(nodeId + " times log full waits on lane " + lane);
                        return LatencyRecorder.NONE;
                    }
                })
                .build();
        journal.writeThrough(node);
        return node;
    }

    private static Deadline stop() {
        return Deadline.of(Duration.ofSeconds(2));
    }

    @AfterEach
    void close() {
        a.close(stop());
        b.close(stop());
    }

    private JournalStream aLeadsBInSync() {
        JournalStream main = journalA.add(MAIN, StreamReplicationMode.SYNC_APPLIED);
        main.append("first");
        a.lead(1, stop());
        b.follow(addressA, () -> 1, stop());
        await().until(() -> b.leader().isInSync() && a.followers().isAnyInSync());
        return main;
    }

    @Test
    void aFollowerIsCaughtUpBySnapshotThenAcksTheLeadersAppends() {
        JournalStream main = aLeadsBInSync();

        main.append("second");

        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(copyB.getWrites())
                .containsExactly("begin from=0 state=2", "snapshot 1 first", "end state=2", "2 second");
        assertThat(a.isLeading()).isTrue();
        assertThat(b.isFollowing()).isTrue();
        assertThat(a.getLogBytesWritten()).isPositive();
        assertThat(recordersAskedFor).containsExactlyInAnyOrder("a times log full waits on lane 0",
                "b times log full waits on lane 0", "a times b lane 0");
        assertThat(catchUps).containsExactly("a caught up b lane 0 after a while");
    }

    @Test
    void aResetStartsTheFollowersCopyOver() {
        JournalStream main = aLeadsBInSync();
        main.append("second");

        main.reset();
        main.append("again");

        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(copyB.getStored()).containsExactly("0 ", "1 again");
    }

    @Test
    void theLeaderSeesItsFollowerInSync() {
        aLeadsBInSync();

        FollowersView followers = a.followers();
        assertThat(followers.countConnected()).isEqualTo(1);
        assertThat(followers.countInSync()).isEqualTo(1);
        assertThat(followers.isStreamInSync(0)).isTrue();
        assertThat(followers.getStreamRefusals(0)).isEmpty();
        assertThat(followers.getSnapshots()).isEqualTo(1);
        assertThat(followers.getSnapshotRecords()).isEqualTo(1);
        assertThat(followers.getDroppedFollowers()).isEmpty();
        assertThat(followers.getDroppedTooSlow()).isZero();
        assertThat(followers.getSendBuffersAllocated()).isZero();
        assertThat(followers.getSendWaits()).isNotNegative();
        assertThat(followers.getLogFullWaits()).isZero();
        assertThat(followers.getCloses()).isEmpty();
        assertThat(followers.getAckTimeouts(0)).isZero();
        assertThat(followers.getAcksWithNoFollowerInSync(0)).isZero();
        assertThat(followers.getLaneStatuses()).singleElement().satisfies(lane -> {
            assertThat(lane.getNodeId()).isEqualTo("b");
            assertThat(lane.isConnected()).isTrue();
            assertThat(lane.isInSync()).isTrue();
            assertThat(lane.getCipher()).isNull();
        });
        await().until(() -> followers.getMaxUnsentBytes() == 0 && followers.getMaxLagNanos() == 0);
    }

    @Test
    void theFollowerSeesItsLeaderAndTheStreamsItHolds() {
        JournalStream main = aLeadsBInSync();
        main.append("second");
        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);

        LeaderView leader = b.leader();
        assertThat(leader.isConnected()).isTrue();
        assertThat(leader.isEncrypted()).isFalse();
        assertThat(leader.isCopyInSync()).isTrue();
        assertThat(leader.getLanes()).isEqualTo(1);
        assertThat(leader.getStatus()).isNotNull().satisfies(status -> {
            assertThat(status.getLeaderId()).isEqualTo("a");
            assertThat(status.isInSync()).isTrue();
        });
        assertThat(leader.getStream(MAIN)).isNotNull().satisfies(stream -> {
            assertThat(stream.isReplicated()).isTrue();
            assertThat(stream.isInSync()).isTrue();
            assertThat(stream.getSink()).isSameAs(copyB);
            assertThat(stream.getLeaderMode()).isEqualTo(StreamReplicationMode.SYNC_APPLIED);
        });
        assertThat(leader.getStream("unknown")).isNull();
        assertThat(leader.getSnapshots()).isEqualTo(1);
        assertThat(leader.getSnapshotRecords()).isEqualTo(1);
        assertThat(leader.getRecordsReceived()).isEqualTo(1);
        assertThat(leader.getBytesReceived()).isPositive();
        assertThat(leader.getReconnects()).isZero();
        assertThat(leader.getCloses()).isEmpty();
    }

    @Test
    void bothSidesCountTheClosesAndTheFollowerItsReconnects() {
        aLeadsBInSync();

        assertThat(a.handOver(stop())).isTrue();
        a.standDown(stop());
        a.lead(1, stop());
        await().until(() -> b.leader().isInSync());
        a.standDown(stop());
        await().until(() -> b.leader().getCloses().containsKey("DISCONNECTED"));

        assertThat(b.leader().getReconnects()).isEqualTo(1);
        assertThat(b.leader().getCloses()).isEqualTo(Map.of("SHUTDOWN", 1L, "DISCONNECTED", 1L));
        assertThat(a.followers().getCloses()).as("its own stop is no lost connection")
                .isEqualTo(Map.of("SHUTDOWN", 1L));
    }

    @Test
    void aLeaderRestartedAtTheSameEpochCatchesItsFollowerUpBySnapshot() {
        JournalStream main = aLeadsBInSync();
        main.append("second");
        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        a.close(stop());
        Journal restartedJournal = new Journal();
        ReplicationNode restarted = node("a", addressA, restartedJournal, new JournalReplica(),
                ReplicationEncryptionSettings.builder().build());
        try {
            JournalStream restartedMain = restartedJournal.add(MAIN, StreamReplicationMode.SYNC_APPLIED);
            restartedMain.append("first");
            restartedMain.append("second");
            restartedMain.append("third");
            copyB.getWrites().clear();

            restarted.lead(1, stop());

            await().untilAsserted(() -> assertThat(copyB.getWrites()).as("its log holds the follower's offset")
                    .startsWith("begin from=0 state=4"));
        } finally {
            restarted.close(stop());
        }
    }

    @Test
    void aFollowerRestartedWithItsCopyIsCaughtUpWithOnlyWhatItMissed() {
        Journal leaderJournal = new Journal();
        ReplicationEncryptionSettings clear = ReplicationEncryptionSettings.builder().build();
        ReplicationNode leader = node("a", addressA, leaderJournal, new JournalReplica(), clear,
                CatchUp.byLastRecords(2));
        ReplicationNode follower = node("b", addressB, new Journal(), replicaB, clear, CatchUp.byLastRecords(2));
        ReplicationNode restarted = node("b", addressB, new Journal(), replicaB, clear, CatchUp.byLastRecords(2));
        try {
            JournalStream main = leaderJournal.add(MAIN, StreamReplicationMode.SYNC_APPLIED);
            main.append("first");
            main.append("second");
            leader.lead(1, stop());
            follower.follow(addressA, () -> 1, stop());
            await().until(() -> follower.leader().isInSync());
            follower.close(stop());
            main.append("third");
            copyB.getWrites().clear();

            restarted.follow(addressA, () -> 1, stop());

            await().untilAsserted(() -> assertThat(copyB.getWrites()).as("its lane positions lost, its copy kept")
                    .containsExactly("begin from=3 state=4", "snapshot 3 third", "end state=4"));
        } finally {
            restarted.close(stop());
            follower.close(stop());
            leader.close(stop());
        }
    }

    /**
     * The follower starts holding {@code followerHolds} from position 1, and catches up with a leader holding
     * {@code leaderHolds} from position 1.
     */
    private JournalCopy caughtUp(CatchUp leaderCatchUp, CatchUp followerCatchUp, List<String> leaderHolds,
                                 List<String> followerHolds) {
        Journal leaderJournal = new Journal();
        JournalReplica followerReplica = new JournalReplica();
        JournalCopy copy = followerReplica.configure(MAIN, StreamReplicationMode.SYNC_APPLIED);
        for (int i = 0; i < followerHolds.size(); i++) {
            copy.hold(i + 1, followerHolds.get(i));
        }
        ReplicationEncryptionSettings clear = ReplicationEncryptionSettings.builder().build();
        ReplicationNode leader = node("n", addressA, leaderJournal, new JournalReplica(), clear, leaderCatchUp);
        ReplicationNode follower = node("o", addressB, new Journal(), followerReplica, clear, followerCatchUp);
        try {
            JournalStream main = leaderJournal.add(MAIN, StreamReplicationMode.SYNC_APPLIED);
            leaderHolds.forEach(main::append);
            leader.lead(1, stop());
            follower.follow(addressA, () -> 1, stop());
            await().until(() -> follower.leader().isInSync());
            return copy;
        } finally {
            follower.close(stop());
            leader.close(stop());
        }
    }

    private static List<String> entries(String prefix, int from, int to) {
        return IntStream.rangeClosed(from, to).mapToObj(position -> prefix + " " + position)
                .collect(Collectors.toList());
    }

    private static List<String> stored(List<String> entries) {
        return IntStream.range(0, entries.size()).mapToObj(i -> (i + 1) + " " + entries.get(i))
                .collect(Collectors.toList());
    }

    private static List<String> concat(List<String> first, List<String> second) {
        List<String> all = new ArrayList<>(first);
        all.addAll(second);
        return all;
    }

    /**
     * The deposed leader's own 8..10 never reached the new leader, which wrote its own at those positions.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aDeposedLeaderHoldingRecordsTheNewOneNeverGotEndsWithTheNewLeadersAlone(boolean byLastRecords) {
        CatchUp catchUp = byLastRecords ? CatchUp.byLastRecords() : CatchUp.fullCopy();
        List<String> leaders = concat(entries("shared", 1, 7), entries("new", 8, 13));

        JournalCopy deposed = caughtUp(catchUp, catchUp, leaders,
                concat(entries("shared", 1, 7), entries("own", 8, 10)));

        assertThat(deposed.getWrites()).first().isEqualTo("begin from=0 state=14");
        assertThat(deposed.getStored()).isEqualTo(stored(leaders));
    }

    @Test
    void aFollowerHoldingTheLeadersFirstRecordsIsCopiedTheRest() {
        List<String> leaders = concat(entries("shared", 1, 7), entries("new", 8, 13));

        JournalCopy follower = caughtUp(CatchUp.byLastRecords(), CatchUp.byLastRecords(), leaders,
                entries("shared", 1, 7));

        assertThat(follower.getWrites()).first().isEqualTo("begin from=8 state=14");
        assertThat(follower.getStored()).isEqualTo(stored(leaders));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aFollowerCatchingUpOtherwiseThanItsLeaderIsCopiedTheWholeStream(boolean leaderByLastRecords) {
        CatchUp leaderCatchUp = leaderByLastRecords ? CatchUp.byLastRecords() : CatchUp.fullCopy();
        CatchUp followerCatchUp = leaderByLastRecords ? CatchUp.fullCopy() : CatchUp.byLastRecords();
        List<String> leaders = entries("shared", 1, 9);

        JournalCopy follower = caughtUp(leaderCatchUp, followerCatchUp, leaders, entries("shared", 1, 7));

        assertThat(follower.getWrites()).first().isEqualTo("begin from=0 state=10");
        assertThat(follower.getStored()).isEqualTo(stored(leaders));
    }

    @Test
    void aStreamTheFollowerRefusesIsReportedToTheLeaderAndNotWaitedFor() {
        replicaB.configure(MAIN, StreamReplicationMode.SYNC_APPLIED, "leader-storage");
        JournalStream main = journalA.add(MAIN, StreamReplicationMode.SYNC_APPLIED);
        a.lead(1, stop());
        b.follow(addressA, () -> 1, stop());
        await().until(() -> !a.followers().getStreamRefusals(0).isEmpty());

        main.append("first");

        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.NO_FOLLOWER_IN_SYNC);
        assertThat(a.followers().getAcksWithNoFollowerInSync(0)).isEqualTo(1);
        assertThat(a.followers().getStreamRefusals(0)).isEqualTo(Map.of("b", StreamAnswer.SHARED_STORAGE));
        assertThat(a.followers().isStreamInSync(0)).isFalse();
        assertThat(b.leader().getStream(MAIN)).isNotNull().satisfies(stream -> {
            assertThat(stream.isReplicated()).isFalse();
            assertThat(stream.getNotReplicatedAnswer()).isEqualTo(StreamAnswer.SHARED_STORAGE);
        });
    }

    @Test
    void aStreamUnknownToTheFollowerIsReplicatedOnceItIsConfigured() {
        JournalStream late = journalA.add("late", StreamReplicationMode.SYNC_APPLIED);
        late.append("first");
        a.lead(1, stop());
        b.follow(addressA, () -> 1, stop());
        await().until(() -> a.followers().getStreamRefusals(0).equals(Map.of("b", StreamAnswer.UNKNOWN_STREAM)));

        JournalCopy lateCopy = replicaB.configure("late", StreamReplicationMode.SYNC_APPLIED);

        await().until(() -> a.followers().isStreamInSync(0));
        late.append("second");
        assertThat(late.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(lateCopy.getWrites())
                .containsExactly("begin from=0 state=2", "snapshot 1 first", "end state=2", "2 second");
    }

    @Test
    void aStreamOfAnotherModeIsReplicatedOnceTheFollowerAgrees() {
        JournalStream main = journalA.add(MAIN, StreamReplicationMode.SYNC_RECEIVED);
        a.lead(1, stop());
        b.follow(addressA, () -> 1, stop());
        await().until(() -> a.followers().getStreamRefusals(0).equals(Map.of("b", StreamAnswer.MODE_MISMATCH)));

        replicaB.configure(MAIN, StreamReplicationMode.SYNC_RECEIVED);

        await().until(() -> a.followers().isStreamInSync(0));
        main.append("first");
        assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(b.leader().getStream(MAIN)).isNotNull().satisfies(stream -> assertThat(stream.isReplicated()).isTrue());
    }

    @Test
    void encryptingNodesReplicateEncryptedAndSayWithWhichCipher() {
        ReplicationEncryptionSettings encryption = ReplicationEncryptionSettings.builder()
                .mode(EncryptionMode.REQUIRED)
                .secret("a-secret-of-the-pair".getBytes(StandardCharsets.UTF_8))
                .build();
        Journal journalC = new Journal();
        JournalReplica replicaD = new JournalReplica();
        JournalCopy copyD = replicaD.configure(MAIN, StreamReplicationMode.SYNC_APPLIED);
        InetSocketAddress addressC = new InetSocketAddress("localhost", TestingPorts.findFree());
        ReplicationNode c = node("c", addressC, journalC, new JournalReplica(), encryption);
        ReplicationNode d = node("d", new InetSocketAddress("localhost", TestingPorts.findFree()), new Journal(),
                replicaD, encryption);
        try {
            JournalStream main = journalC.add(MAIN, StreamReplicationMode.SYNC_APPLIED);
            c.lead(1, stop());
            d.follow(addressC, () -> 1, stop());
            await().until(() -> d.leader().isInSync() && c.followers().isAnyInSync());

            main.append("first");

            assertThat(main.awaitAck(ACK_TIMEOUT)).isEqualTo(StreamWriter.Ack.ACKED);
            assertThat(copyD.getWrites()).endsWith("1 first");
            assertThat(d.leader().isEncrypted()).isTrue();
            assertThat(d.leader().getStatus()).isNotNull()
                    .satisfies(status -> assertThat(status.getCipher()).isEqualTo(c.describeCipher()));
            assertThat(c.followers().getLaneStatuses()).singleElement()
                    .satisfies(lane -> assertThat(lane.getCipher()).isEqualTo(c.describeCipher()));
        } finally {
            c.close(stop());
            d.close(stop());
        }
    }

    @Test
    void aResyncSendsTheStreamAgainBySnapshot() {
        aLeadsBInSync();
        copyB.getWrites().clear();

        a.resync(0);

        await().untilAsserted(() -> assertThat(copyB.getWrites()).startsWith("begin from=0 state=2"));
        assertThat(a.followers().getSnapshots()).isEqualTo(2);
    }

    @Test
    void aFollowerHandedOverTakesOverWhatItHoldsOnceItsSinksStop() {
        aLeadsBInSync();

        assertThat(a.handOver(stop())).isTrue();
        await().until(() -> b.leader().getStream(MAIN).isHandedOver());
        a.standDown(stop());
        b.lead(2, stop());
        a.follow(addressB, () -> 2, stop());

        assertThat(copyB.isStopped()).isTrue();
        assertThat(b.leader().getStream(MAIN)).as("the copy is the application's now").isNull();
        assertThat(a.isLeading()).isFalse();
        await().until(() -> a.leader().isConnected() && b.followers().countConnected() == 1);
    }

    @Test
    void aNodeStandingDownNeitherLeadsNorFollows() {
        aLeadsBInSync();

        a.standDown(stop());
        b.standDown(stop());

        assertThat(a.isLeading()).isFalse();
        assertThat(a.handOver(stop())).isFalse();
        assertThat(a.followers().getLaneStatuses()).isEmpty();
        assertThat(b.isFollowing()).isFalse();
        assertThat(b.leader().getStatus()).isNull();
        assertThat(b.leader().isConnected()).isFalse();
        assertThat(b.leader().isInSync()).isFalse();
        assertThat(b.leader().isCopyInSync()).as("what it holds is kept").isTrue();
        assertThat(copyB.isStopped()).isFalse();
    }

    @Test
    void aWriterRefusesAPayloadLongerThanHalfTheLog() {
        try (StreamWriter writer = a.writer(0)) {
            ByteBuffer tooLong = ByteBuffer.allocate(writer.getMaxPayloadLength() + 1);

            assertThat(writer.getMaxPayloadLength()).isEqualTo(1 << 15);
            assertThatThrownBy(() -> writer.append(JournalStream.TEXT, RecordFlag.NONE, 1, tooLong, 0, tooLong.capacity()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(a.getLogBytesWritten()).isZero();
        }
    }

    @Test
    void aNodeIsBuiltWithItsDefaults() {
        assertThat(a.getLanes()).isEqualTo(1);
        assertThat(a.cipherAllocatesOnEveryFrame()).isFalse();
        assertThat(a.describeCipher()).isNotEmpty();
    }

    @Test
    void aNodeIsNotBuiltWithoutWhatItNeeds() {
        assertThatThrownBy(() -> ReplicationNode.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("nodeId is required");
        assertThatThrownBy(() -> ReplicationNode.builder().nodeId("c").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("leader or follower settings are required");
        assertThatThrownBy(() -> LeaderSettings.builder().streams(new Journal()).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("bindAddress");
    }

    @Test
    void aNodeFollowsNoLeaderWithoutAnAddress() {
        assertThatThrownBy(() -> b.follow(List.of(), () -> 1, stop()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No leader address to follow");
    }

    @Test
    void aNodeTakesOnlyTheRolesItHasSettingsFor() {
        ReplicationNode leaderOnly = ReplicationNode.builder()
                .nodeId("c")
                .leader(LeaderSettings.builder().bindAddress(addressA).streams(new Journal()).build())
                .build();
        ReplicationNode followerOnly = ReplicationNode.builder()
                .nodeId("d")
                .follower(FollowerSettings.builder().sinks(new JournalReplica()).build())
                .build();
        try {
            assertThatThrownBy(() -> leaderOnly.follow(addressB, () -> 1, stop()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Node c has no follower settings: it cannot follow");
            assertThatThrownBy(() -> followerOnly.lead(1, stop()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Node d has no leader settings: it cannot lead");
            assertThatThrownBy(() -> followerOnly.writer(0))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(followerOnly.followers().countConnected()).isZero();
        } finally {
            leaderOnly.close(stop());
            followerOnly.close(stop());
        }
    }
}
