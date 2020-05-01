/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import com.linkedin.kafka.clients.utils.PrimitiveEncoderDecoder;
import java.util.UUID;


/**
 * This class represents the header value for a large message.
 * Every large message header takes up 29 bytes and is structured as follows
 *
 * | Type   | UUID     | segmentNumber | numberOfSegments | messageSizeInBytes |
 * | 1 byte | 16 bytes | 4 bytes       | 4 bytes          | 4 bytes            |
 *
 * The Large message header values will be used to support large messages eventually.
 * (as opposed to encoding large segment metadata info inside the payload)
 */
public class LargeMessageHeaderValue {
  public static final UUID EMPTY_UUID = new UUID(0L, 0L);
  public static final int INVALID_SEGMENT_ID = -1;
  public static final int INVALID_MESSAGE_SIZE = -1;
  private final byte _type;
  private final UUID _uuid;
  private final int _segmentNumber;
  private final int _numberOfSegments;
  private final int _messageSizeInBytes;

  // This indicates that the large message framework is using
  // SegmentSerializer/SegmentDeserializer interface to split
  // and assemble large message segments.
  public static final byte LEGACY = (byte) 0;
  // This indicates that the segment metadata of a large message can be found in the record header and not in the payload
  public static final byte LEGACY_V2 = (byte) 1;

  public LargeMessageHeaderValue(byte type, UUID uuid, int segmentNumber, int numberOfSegments, int messageSizeInBytes) {
    _type = type;
    _uuid = uuid;
    _segmentNumber = segmentNumber;
    _numberOfSegments = numberOfSegments;
    _messageSizeInBytes = messageSizeInBytes;
  }

  public int getMessageSizeInBytes() {
    return _messageSizeInBytes;
  }

  public int getSegmentNumber() {
    return _segmentNumber;
  }

  public int getNumberOfSegments() {
    return _numberOfSegments;
  }

  public UUID getUuid() {
    return _uuid;
  }

  public byte getType() {
    return _type;
  }

  public static byte[] toBytes(LargeMessageHeaderValue largeMessageHeaderValue) {
    byte[] serialized = largeMessageHeaderValue.getType() == LEGACY ? new byte[25] : new byte[29];

    int byteOffset = 0;
    serialized[byteOffset] = largeMessageHeaderValue.getType();
    byteOffset += 1; // for type
    PrimitiveEncoderDecoder.encodeLong(largeMessageHeaderValue.getUuid().getLeastSignificantBits(), serialized, byteOffset);
    byteOffset += PrimitiveEncoderDecoder.LONG_SIZE; // for UUID(least significant bits)
    PrimitiveEncoderDecoder.encodeLong(largeMessageHeaderValue.getUuid().getMostSignificantBits(), serialized, byteOffset);
    byteOffset += PrimitiveEncoderDecoder.LONG_SIZE; // for UUID(most significant bits)
    PrimitiveEncoderDecoder.encodeInt(largeMessageHeaderValue.getSegmentNumber(), serialized, byteOffset);
    byteOffset += PrimitiveEncoderDecoder.INT_SIZE; // for segment number
    PrimitiveEncoderDecoder.encodeInt(largeMessageHeaderValue.getNumberOfSegments(), serialized, byteOffset);
    if (largeMessageHeaderValue.getType() == LEGACY_V2) { // We serialize the new field - messageSize
      byteOffset += PrimitiveEncoderDecoder.INT_SIZE; // for message size
      PrimitiveEncoderDecoder.encodeInt(largeMessageHeaderValue.getMessageSizeInBytes(), serialized, byteOffset);
    }
    return serialized;
  }

  public static LargeMessageHeaderValue fromBytes(byte[] bytes) {
    int byteOffset = 0;

    byte type = bytes[byteOffset];
    byteOffset += 1;
    long leastSignificantBits = PrimitiveEncoderDecoder.decodeLong(bytes, byteOffset);
    byteOffset += PrimitiveEncoderDecoder.LONG_SIZE;
    long mostSignificantBits = PrimitiveEncoderDecoder.decodeLong(bytes, byteOffset);
    byteOffset += PrimitiveEncoderDecoder.LONG_SIZE;
    int segmentNumber = PrimitiveEncoderDecoder.decodeInt(bytes, byteOffset);
    byteOffset += PrimitiveEncoderDecoder.INT_SIZE;
    int numberOfSegments = PrimitiveEncoderDecoder.decodeInt(bytes, byteOffset);
    if (bytes.length == 29) {
      byteOffset += PrimitiveEncoderDecoder.INT_SIZE;
      int messageSizeInBytes = PrimitiveEncoderDecoder.decodeInt(bytes, byteOffset);
      return new LargeMessageHeaderValue(type, new UUID(mostSignificantBits, leastSignificantBits), segmentNumber, numberOfSegments, messageSizeInBytes);
    }
    return new LargeMessageHeaderValue(type, new UUID(mostSignificantBits, leastSignificantBits), segmentNumber, numberOfSegments, INVALID_MESSAGE_SIZE);
  }
}
