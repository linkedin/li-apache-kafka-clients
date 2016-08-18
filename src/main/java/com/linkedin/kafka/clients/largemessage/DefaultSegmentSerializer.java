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

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * The default large message segment serializer.
 * The format of the serialized segment is:
 * 1 byte   - version
 * 4 bytes  - checksum to determine if bytes is large message segment or not.
 * 16 bytes - messageId
 * 4 bytes  - sequence number
 * 4 bytes  - number of segments
 * 4 bytes  - message size in bytes
 * X bytes  - payload
 */
public class DefaultSegmentSerializer implements Serializer<LargeMessageSegment> {
  private final int CHECKSUM_LENGTH = Integer.BYTES;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public byte[] serialize(String s, LargeMessageSegment segment) {
    if (segment.numberOfSegments > 1) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(1 + LargeMessageSegment.SEGMENT_INFO_OVERHEAD +
          segment.payload.limit() + CHECKSUM_LENGTH);
      byteBuffer.put(LargeMessageSegment.CURRENT_VERSION);
      byteBuffer.putInt((int) (segment.messageId.getMostSignificantBits() + segment.messageId.getLeastSignificantBits()));
      byteBuffer.putLong(segment.messageId.getMostSignificantBits());
      byteBuffer.putLong(segment.messageId.getLeastSignificantBits());
      byteBuffer.putInt(segment.sequenceNumber);
      byteBuffer.putInt(segment.numberOfSegments);
      byteBuffer.putInt(segment.messageSizeInBytes);
      byteBuffer.put(segment.payload.array());
      return byteBuffer.array();
    } else {
      return segment.payload.array();
    }
  }

  @Override
  public void close() {

  }
}
