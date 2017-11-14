/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

/**
 * Default deserializer for large message segment
 */
public class DefaultSegmentDeserializer implements Deserializer<LargeMessageSegment> {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultSegmentDeserializer.class);
  private static final int CHECKSUM_LENGTH = Integer.BYTES;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public LargeMessageSegment deserialize(String s, byte[] bytes) {
    int headerLength = 1 + LargeMessageSegment.SEGMENT_INFO_OVERHEAD + CHECKSUM_LENGTH;
    if (bytes.length < headerLength) {
      LOG.debug("Serialized segment size too small, not large message segment.");
      return null;
    }
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byte version = byteBuffer.get();
    if (version > LargeMessageSegment.CURRENT_VERSION) {
      LOG.debug("Serialized version byte is greater than {}. not large message segment.",
          LargeMessageSegment.CURRENT_VERSION);
      return null;
    }
    int checksum = byteBuffer.getInt();
    long messageIdMostSignificantBits = byteBuffer.getLong();
    long messageIdLeastSignificantBits = byteBuffer.getLong();
    if (checksum == 0 ||
        checksum != ((int) (messageIdMostSignificantBits + messageIdLeastSignificantBits))) {
      LOG.debug("Serialized segment checksum does not match. not large message segment.");
      return null;
    }
    UUID messageId = new UUID(messageIdMostSignificantBits, messageIdLeastSignificantBits);
    int sequenceNumber = byteBuffer.getInt();
    int numberOfSegments = byteBuffer.getInt();
    int messageSizeInBytes = byteBuffer.getInt();
    ByteBuffer payload = byteBuffer.slice();
    return new LargeMessageSegment(messageId, sequenceNumber, numberOfSegments, messageSizeInBytes, payload);
  }

  @Override
  public void close() {

  }
}
