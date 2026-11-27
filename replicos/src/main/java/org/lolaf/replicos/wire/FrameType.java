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
package org.lolaf.replicos.wire;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.lolaf.replicos.spi.RecordFlag;
import org.lolaf.replicos.spi.WireFormatException;

/**
 * The frames of the replication stream, each with the size of its fixed payload. A frame is the header, its fixed
 * payload, then a variable part (its strings, UTF-8 each as long as a length field of the fixed payload says, a
 * record's payload, a stream's state). Every field's offset and type is in {@code docs/protocol.md}, the reference: a
 * change to a frame changes it in the same commit.
 *
 * <p>A record's content type is the application's, never read by the replication; its flags are
 * {@link WireFormat#MORE_FRAGMENTS}, {@link WireFormat#CONTINUED_FRAGMENT}, {@link RecordFlag#PLAINTEXT} and
 * {@link RecordFlag#RESET}. A stream's state is opaque bytes too, which a snapshot carries for the application to
 * resume the stream from.
 *
 * <p>On an encrypted connection the frames after the handshake end with a tag, whatever their type; see
 * {@link WireFormat}.
 */
@Getter
@RequiredArgsConstructor
public enum FrameType {

    HELLO(1, 56),
    WELCOME(2, 48),
    STREAM(3, 10),
    STREAM_ANSWER(4, 16),
    RECORD(5, 32),
    SNAPSHOT_BEGIN(6, 24),
    SNAPSHOT_RECORD(7, 24),
    SNAPSHOT_END(8, 12),
    ACK(9, 16),
    HEARTBEAT(10, 8),
    CLOSE(11, 4),
    RECEIVED(12, 8),
    STREAMS_ANNOUNCED(13, 8),
    KEY_CONFIRMATION(14, 32);

    private static final FrameType[] BY_CODE = new FrameType[256];

    static {
        for (FrameType type : values()) {
            BY_CODE[type.code] = type;
        }
    }

    private final int code;
    private final int fixedPayloadBytes;

    /**
     * @throws WireFormatException for a code no version of the format defines
     */
    public static FrameType from(int code) {
        FrameType type = BY_CODE[code & 0xFF];
        if (type == null) {
            throw new WireFormatException("Unknown frame type " + (code & 0xFF));
        }
        return type;
    }
}
