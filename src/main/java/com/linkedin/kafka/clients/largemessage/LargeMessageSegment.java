/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.largemessage;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * The class that holds a large message segment.
 */
public class LargeMessageSegment {
  public final UUID messageId;
  public final int sequenceNumber;
  public final int numberOfSegments;
  public final int messageSizeInBytes;
  public final ByteBuffer payload;
  // The segment information over head bytes when serialize.
  public static final int SEGMENT_INFO_OVERHEAD = 16 + Integer.BYTES + Integer.BYTES + Integer.BYTES;
  public static final byte CURRENT_VERSION = 0;

  public LargeMessageSegment(UUID messageId,
                             int sequenceNumber,
                             int numberOfSegments,
                             int messageSizeInBytes,
                             ByteBuffer payload) {
    this.messageId = messageId;
    this.sequenceNumber = sequenceNumber;
    this.numberOfSegments = numberOfSegments;
    this.messageSizeInBytes = messageSizeInBytes;
    this.payload = payload;
  }

  /**
   * Notice that the payload as a ByteBuffer does not guarantee to have a dedicated underlying byte array. So calling
   * {@code payload.array()} will not always give the payload byte array. This method should be called if user wants
   * to have the payload byte array.
   *
   * @return The payload as a byte array.
   */
  public byte[] payloadArray() {
    if (payload.arrayOffset() == 0 && payload.limit() == payload.array().length) {
      return payload.array();
    } else {
      return Arrays.copyOfRange(payload.array(), payload.arrayOffset(), payload.arrayOffset() + payload.limit());
    }
  }

  @Override
  public String toString() {
    return "[messageId=" + messageId + ",seq=" + sequenceNumber + ",numSegs=" + numberOfSegments + ",messageSize=" +
        messageSizeInBytes + ",payloadSize=" + payload.limit() + "]";
  }
}
