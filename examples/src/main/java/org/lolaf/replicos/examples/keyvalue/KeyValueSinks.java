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
import org.lolaf.replicos.spi.ReplicaSinks;
import org.lolaf.replicos.spi.StreamAnswer;
import org.lolaf.replicos.spi.StreamReplicationMode;

/**
 * On a follower, applies the leader's stream into this node's store, unless the leader's is the same store.
 */
final class KeyValueSinks implements ReplicaSinks {

    private final KeyValueStore store;
    private final String storageIdentity;

    KeyValueSinks(KeyValueStore store, String storageIdentity) {
        this.store = store;
        this.storageIdentity = storageIdentity;
    }

    @Override
    public int getApplicationId() {
        return KeyValueNode.APPLICATION_ID;
    }

    @Override
    public Binding open(String streamName, StreamReplicationMode leaderMode, @Nullable String leaderStorageIdentity) {
        if (!KeyValueStream.NAME.equals(streamName)) {
            return Binding.refused(StreamAnswer.UNKNOWN_STREAM);
        }
        if (leaderMode != KeyValueStream.MODE) {
            return Binding.refused(StreamAnswer.MODE_MISMATCH);
        }
        if (leaderStorageIdentity == null) {
            return Binding.refused(StreamAnswer.UNKNOWN_STORAGE);
        }
        if (leaderStorageIdentity.equals(storageIdentity)) {
            return Binding.refused(StreamAnswer.SHARED_STORAGE);
        }
        return Binding.replicated(new KeyValueSink(store));
    }

    @Override
    public @Nullable StreamReplicationMode modeHere(String streamName) {
        return KeyValueStream.NAME.equals(streamName) ? KeyValueStream.MODE : null;
    }
}
