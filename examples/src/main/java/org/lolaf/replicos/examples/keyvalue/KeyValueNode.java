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

import org.jspecify.annotations.Nullable;
import org.lolaf.replicos.*;
import org.lolaf.ringos.Deadline;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;

/**
 * A key-value store on a replication node, which leads or follows as it is told: with no election here, the epoch
 * is the caller's.
 */
public final class KeyValueNode implements AutoCloseable {

    /**
     * A node of another cluster is refused.
     */
    public static final String CLUSTER = "key-value-example";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final KeyValueStore store;
    private final ReplicationNode node;
    private final RegisteredStream registered;
    private final KeyValueStream stream;

    public KeyValueNode(String nodeId, InetSocketAddress bindAddress) {
        String storageIdentity = "memory of " + nodeId;
        this.store = new KeyValueStore();
        this.node = ReplicationNode.builder()
                .nodeId(nodeId)
                .cluster(CLUSTER)
                .leader(LeaderSettings.builder()
                        .bindAddress(bindAddress)
                        .build())
                .follower(FollowerSettings.builder()
                        .sinks(new KeyValueSinks(store, storageIdentity))
                        .build())
                .catchUp(CatchUp.byLastRecords())
                .build();
        this.registered = node.register(KeyValueStream.settings(store, storageIdentity));
        this.stream = new KeyValueStream(store, registered.writer());
    }

    public void lead(long epoch) {
        node.lead(epoch, Deadline.of(TIMEOUT));
    }

    public void follow(InetSocketAddress leaderAddress, long epoch) {
        node.follow(leaderAddress, () -> epoch, Deadline.of(TIMEOUT));
    }

    /**
     * Stops leading or following, keeping the store, as when another node is elected.
     */
    public void standDown() {
        node.standDown(Deadline.of(TIMEOUT));
    }

    public boolean isLeading() {
        return node.isLeading();
    }

    /**
     * On the leader, whether a follower holds the stream in sync; on a follower, whether it does.
     */
    public boolean isInSync() {
        return node.isLeading() ? registered.isInSync() : node.leader().isInSync();
    }

    /**
     * On the leader, from one thread: returns once a follower applied the change, or why it did not.
     */
    public StreamWriter.Ack put(String key, String value) {
        return stream.write(Change.put(key, value));
    }

    public StreamWriter.Ack remove(String key) {
        return stream.write(Change.remove(key));
    }

    public @Nullable String get(String key) {
        return store.get(key);
    }

    public Map<String, String> entries() {
        return store.entries();
    }

    @Override
    public void close() {
        node.close(Deadline.of(TIMEOUT));
    }
}
