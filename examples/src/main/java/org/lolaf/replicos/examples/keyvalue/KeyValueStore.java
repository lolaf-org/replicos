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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * The application's own storage: the entries, and the journal of the changes that made them, which a snapshot reads
 * back to catch a follower up. In memory, for the example; replicos never sees it.
 */
final class KeyValueStore {

    private final Map<String, String> entries;
    private final NavigableMap<Long, Change> journal;

    KeyValueStore() {
        this.entries = new TreeMap<>();
        this.journal = new TreeMap<>();
    }

    synchronized void apply(long position, Change change) {
        journal.put(position, change);
        if (change.getKind() == Change.PUT) {
            entries.put(change.getKey(), change.getValue());
        } else {
            entries.remove(change.getKey());
        }
    }

    synchronized void clear() {
        journal.clear();
        entries.clear();
    }

    synchronized long lastPosition() {
        return journal.isEmpty() ? 0 : journal.lastKey();
    }

    synchronized List<Map.Entry<Long, Change>> changes(long fromPosition, long toPosition) {
        return new ArrayList<>(journal.subMap(fromPosition, true, toPosition, true).entrySet());
    }

    synchronized @Nullable String get(String key) {
        return entries.get(key);
    }

    synchronized Map<String, String> entries() {
        return new TreeMap<>(entries);
    }
}
