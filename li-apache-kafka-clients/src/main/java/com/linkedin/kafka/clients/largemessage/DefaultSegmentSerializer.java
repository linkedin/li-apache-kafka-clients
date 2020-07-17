/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
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
  // 1 is for version byte; Integer.BYTES is for checksum; SEGMENT_INFO_OVERHEAD is for other metadata
  public static final int PAYLOAD_HEADER_OVERHEAD = 1 + Integer.BYTES  + LargeMessageSegment.SEGMENT_INFO_OVERHEAD;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public byte[] serialize(String s, LargeMessageSegment segment) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(PAYLOAD_HEADER_OVERHEAD + segment.payload.limit());
    byteBuffer.put(LargeMessageSegment.CURRENT_VERSION);
    byteBuffer.putInt((int) (segment.messageId.getMostSignificantBits() + segment.messageId.getLeastSignificantBits()));
    byteBuffer.putLong(segment.messageId.getMostSignificantBits());
    byteBuffer.putLong(segment.messageId.getLeastSignificantBits());
    byteBuffer.putInt(segment.sequenceNumber);
    byteBuffer.putInt(segment.numberOfSegments);
    byteBuffer.putInt(segment.messageSizeInBytes);
    byteBuffer.put(segment.payload);
    return byteBuffer.array();
  }

  @Override
  public void close() {

  }
}
