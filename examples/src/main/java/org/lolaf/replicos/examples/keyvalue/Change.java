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

import lombok.Value;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * One change to the store, replicated as one record: its kind is the record's content type, and its payload the key's
 * length, the key and the value, in UTF-8.
 */
@Value
class Change {

    static final byte PUT = 1;
    static final byte REMOVE = 2;

    byte kind;
    String key;
    @Nullable String value;

    static Change put(String key, String value) {
        return new Change(PUT, key, value);
    }

    static Change remove(String key) {
        return new Change(REMOVE, key, null);
    }

    static Change decode(byte kind, ByteBuffer buffer, int index, int length) {
        byte[] bytes = new byte[length];
        buffer.duplicate().position(index).get(bytes);
        int keyLength = ByteBuffer.wrap(bytes).getShort();
        String key = new String(bytes, Short.BYTES, keyLength, StandardCharsets.UTF_8);
        if (kind == REMOVE) {
            return remove(key);
        }
        int valueIndex = Short.BYTES + keyLength;
        return put(key, new String(bytes, valueIndex, length - valueIndex, StandardCharsets.UTF_8));
    }

    ByteBuffer encode() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(Short.BYTES + keyBytes.length + valueBytes.length)
                .putShort((short) keyBytes.length)
                .put(keyBytes)
                .put(valueBytes)
                .flip();
    }

    @Override
    public String toString() {
        return kind == PUT ? "put " + key + "=" + value : "remove " + key;
    }
}
