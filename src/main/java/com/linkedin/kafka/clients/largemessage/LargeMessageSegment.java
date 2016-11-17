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
import java.util.Map;
import java.util.UUID;

/**
 * The class that holds a large message segment.
 * <p>
 * Each large message segment contains the following information:
 * <ul>
 *   <li><b>MessageId:</b> The message id of the original large message.</li>
 *   <li><b>SequenceNumber:</b> The sequence number of the segment.</li>
 *   <li><b>NumberOfSegments:</b> The total number of segments the original large message has.</li>
 *   <li><b>originalValueSize:</b> The size of the original large message in bytes.</li>
 *   <li><b>segment:</b> The ByteBuffer of the segment.  The segment is the value part of the original producer.</li>
 * </ul>
 *
 * TODO: do I need this method?
 * Please notice that it is not guaranteed that the payload ByteBuffer has a dedicated underlying byte array. To
 * get a dedicated byte array representation of the payload, {@link #payloadArray()} method should be called.
 *
 */
public class LargeMessageSegment {
  // The segment information over head bytes when serialize.
  public static final int SEGMENT_INFO_OVERHEAD = 16 + Integer.BYTES + Integer.BYTES + Integer.BYTES;
  /**
   * Version history:
   * 0 - Version 0 did not use headers.
   */
  public static final byte CURRENT_VERSION = 1;

  private final UUID _messageId;
  private final int _sequenceNumber;
  private final int _numberOfSegments;
  private final int _originalValueSize;

  private final ByteBuffer _segment;

  /**
   *  This constructor should be used by the producer when segmenting a large message into segments.
   *
   * @param segment position should be assigned to the start of the segment, limit the end of the segment, remaining()
   *                should give the number of bytes in the segment
   */
  public LargeMessageSegment(UUID messageId,
                             int sequenceNumber,
                             int numberOfSegments,
                             int originalValueSize,
                             ByteBuffer segment) {
    this._messageId = messageId;
    this._sequenceNumber = sequenceNumber;
    this._numberOfSegments = numberOfSegments;
    this._originalValueSize = originalValueSize;
    this._segment = segment;
  }

  /**
   * This constructor should be used when consuming a record from the broker.
   *
   * @param headerPayload non-null, this was originally generated from the producer side.  This is stored in the header
   *                      map with key {@value com.linkedin.kafka.clients.consumer.HeaderKeySpace#LARGE_MESSAGE_SEGMENT_HEADER}
   * @param segment the bytes for this segment
   */
  public LargeMessageSegment(byte[] headerPayload, ByteBuffer segment) {
    ByteBuffer headerReader = ByteBuffer.wrap(headerPayload);
    byte headerVersion = headerReader.get();
    if (headerVersion != CURRENT_VERSION) {
      throw new IllegalArgumentException("Invalid large message header, expected version " + CURRENT_VERSION +
        " but found version " + headerVersion + ".");
    }
    long uuidMsb = headerReader.getLong();
    long uuidLsb = headerReader.getLong();
    this._messageId = new UUID(uuidMsb, uuidLsb);
    this._sequenceNumber = headerReader.getInt();
    if (_sequenceNumber < 0) {
      throw new IllegalArgumentException("Sequence number must be non-negative.");
    }
    this._numberOfSegments = headerReader.getInt();
    if (_numberOfSegments < 0) {
      throw new IllegalArgumentException("Number of segments must be non-negative.");
    }
    if (_numberOfSegments <= _sequenceNumber) {
      throw new IllegalArgumentException("Number of segments must be greater than sequence number.");
    }
    this._originalValueSize = headerReader.getInt();
    if (_originalValueSize < 0) {
      throw new IllegalArgumentException("Original value size must be non-negative.");
    }
    this._segment = segment;

  }

  @Override
  public String toString() {
    return "[messageId=" + _messageId + ",seq=" + _sequenceNumber + ",numSegs=" + _numberOfSegments +
      ",originalValueSize=" + _originalValueSize + ",segmentSize=" + _segment.remaining() + "]";
  }


  byte[] segmentHeader() {
    ByteBuffer headerValue =
      ByteBuffer.allocate(1 /* version*/ + 8 + 8 /* uuid */ + 4 /* sequenceNumber */ + 4 /* numberOfSegments */ +
      4 /* originalValueSize */);
    headerValue.put(CURRENT_VERSION);
    headerValue.putLong(_messageId.getMostSignificantBits());
    headerValue.putLong(_messageId.getLeastSignificantBits());
    headerValue.putInt(_sequenceNumber);
    headerValue.putInt(_numberOfSegments);
    headerValue.putInt(_originalValueSize);
    return headerValue.array();
  }
}
