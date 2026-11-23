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
package org.lolaf.replicos.benchmarks.impl;

import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;

@UtilityClass
class Payloads {

    /**
     * Into {@code target} from 0, by absolute reads: {@code source} is the library's, left untouched, and a bulk get at
     * an index needs Java 13.
     */
    static void copy(ByteBuffer source, int index, int length, ByteBuffer target) {
        int i = 0;
        for (; i + Long.BYTES <= length; i += Long.BYTES) {
            target.putLong(i, source.getLong(index + i));
        }
        for (; i < length; i++) {
            target.put(i, source.get(index + i));
        }
    }
}
