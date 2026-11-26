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
package org.lolaf.replicos.examples.keyvalue;

import com.github.valfirst.slf4jtest.LoggingEvent;
import com.github.valfirst.slf4jtest.TestLoggerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lolaf.replicos.StreamWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class TestKeyValueNode {

    private final InetSocketAddress leaderAddress;
    private final List<KeyValueNode> nodes;

    TestKeyValueNode() {
        this.leaderAddress = new InetSocketAddress("localhost", freePort());
        this.nodes = new ArrayList<>();
    }

    @AfterEach
    void close() {
        nodes.forEach(KeyValueNode::close);
    }

    @Test
    void aChangeIsOnTheFollowerOnceAcked() {
        KeyValueNode leader = leader();
        KeyValueNode follower = followerOf(leader);

        assertThat(leader.put("colour", "blue")).isEqualTo(StreamWriter.Ack.ACKED);

        assertThat(follower.get("colour")).isEqualTo("blue");
    }

    @Test
    void aLateFollowerCatchesUpBySnapshot() {
        KeyValueNode leader = leader();
        assertThat(leader.put("colour", "blue")).isEqualTo(StreamWriter.Ack.NO_FOLLOWER_IN_SYNC);
        leader.put("size", "large");
        leader.remove("colour");

        KeyValueNode follower = followerOf(leader);

        assertThat(follower.entries()).isEqualTo(Map.of("size", "large"));
    }

    @Test
    void aDeposedLeaderFollowingTheNewOneGetsWhatItMissedAndKeepsTheRest() {
        KeyValueNode first = leader();
        InetSocketAddress secondAddress = new InetSocketAddress("localhost", freePort());
        KeyValueNode second = start("second", secondAddress);
        second.follow(leaderAddress, 1);
        await().atMost(Duration.ofSeconds(10)).until(() -> second.isInSync() && first.isInSync());
        assertThat(first.put("colour", "blue")).isEqualTo(StreamWriter.Ack.ACKED);
        assertThat(first.put("size", "large")).isEqualTo(StreamWriter.Ack.ACKED);
        first.standDown();
        second.standDown();
        second.lead(2);
        second.put("shape", "round");
        TestLoggerFactory.clearAll();

        first.follow(secondAddress, 2);

        await().atMost(Duration.ofSeconds(10)).until(() -> first.isInSync() && second.isInSync());
        assertThat(first.entries()).isEqualTo(Map.of("colour", "blue", "size", "large", "shape", "round"));
        assertThat(TestLoggerFactory.getTestLogger(KeyValueSink.class).getAllLoggingEvents())
                .extracting(LoggingEvent::getFormattedMessage)
                .as("copied from the change it missed")
                .contains("Snapshot started from position 3, the store kept before it");
    }

    private KeyValueNode leader() {
        KeyValueNode leader = start("leader", leaderAddress);
        leader.lead(1);
        return leader;
    }

    private KeyValueNode followerOf(KeyValueNode leader) {
        KeyValueNode follower = start("follower", new InetSocketAddress("localhost", freePort()));
        follower.follow(leaderAddress, 1);
        await().atMost(Duration.ofSeconds(10)).until(() -> follower.isInSync() && leader.isInSync());
        return follower;
    }

    private KeyValueNode start(String nodeId, InetSocketAddress address) {
        KeyValueNode node = new KeyValueNode(nodeId, address);
        nodes.add(node);
        return node;
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
