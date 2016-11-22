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

import com.linkedin.kafka.clients.largemessage.errors.InvalidSegmentException;
import java.nio.ByteBuffer;
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
 * Please notice that it is not guaranteed that the payload ByteBuffer has a dedicated underlying byte array. To
 * get a dedicated byte array representation of the payload, {@link #segmentArray()} method should be called.
 *
 */
public class LargeMessageSegment {

  /**
   * Version history:
   * 0 - did not use headers.
   * 1 - segment metadata is stored in a header
   */
  public static final byte CURRENT_VERSION = 1;

  private final UUID _messageId;
  private final int _sequenceNumber;
  private final int _numberOfSegments;
  private final int _originalValueSize;
  private final boolean _originalKeyWasNull;
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
                             boolean originalKeyWasNull,
                             ByteBuffer segment) {
    if (messageId == null) {
      throw new InvalidSegmentException("messageId must not be null.");
    }
    this._messageId = messageId;

    if (sequenceNumber < 0) {
      throw new InvalidSegmentException("sequenceNumber must be non-negative.");
    }
    this._sequenceNumber = sequenceNumber;

    if (sequenceNumber >= numberOfSegments || numberOfSegments < 0) {
      throw new InvalidSegmentException("Sequence number " + sequenceNumber
        + " should fall between [0," + (numberOfSegments - 1) + "].");
    }
    this._numberOfSegments = numberOfSegments;

    if (originalValueSize < 0) {
      throw new InvalidSegmentException("originalValueSize must be non-negative.");
    }
    this._originalValueSize = originalValueSize;

    if (segment == null) {
      throw new InvalidSegmentException("segment ByteBuffer must not be null");
    }
    this._segment = segment;

    this._originalKeyWasNull = originalKeyWasNull;
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
    if (segment == null) {
      throw new IllegalArgumentException("segment ByteBuffer must not be null");
    }
    if (headerPayload == null) {
      throw new IllegalArgumentException("headerPayload must not be null");
    }
    long uuidMsb = headerReader.getLong();
    long uuidLsb = headerReader.getLong();
    this._messageId = new UUID(uuidMsb, uuidLsb);
    this._sequenceNumber = headerReader.getInt();
    if (_sequenceNumber < 0) {
      throw new InvalidSegmentException("Sequence number must be non-negative.");
    }
    this._numberOfSegments = headerReader.getInt();
    if (_numberOfSegments < 0) {
      throw new InvalidSegmentException("Number of segments must be non-negative.");
    }
    if (_numberOfSegments <= _sequenceNumber) {
      throw new InvalidSegmentException("Number of segments must be greater than sequence number.");
    }
    this._originalValueSize = headerReader.getInt();
    if (_originalValueSize < 0) {
      throw new InvalidSegmentException("Original value size must be non-negative.");
    }
    this._originalKeyWasNull = headerReader.get() == 1;
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
      4 /* originalValueSize */ + 1 /* originalWasNull */);
    headerValue.put(CURRENT_VERSION);
    headerValue.putLong(_messageId.getMostSignificantBits());
    headerValue.putLong(_messageId.getLeastSignificantBits());
    headerValue.putInt(_sequenceNumber);
    headerValue.putInt(_numberOfSegments);
    headerValue.putInt(_originalValueSize);
    headerValue.put((byte) (_originalKeyWasNull ? 1 : 0));
    return headerValue.array();
  }


  ByteBuffer segmentByteBuffer() {
    return _segment;
  }

  UUID messageId() {
    return _messageId;
  }

  /**
   * The size of the original, serialized value before segmentation.
   * @return non-negative
   */
  int originalValueSize() {
    return _originalValueSize;
  }

  int numberOfSegments() {
    return _numberOfSegments;
  }

  byte[] segmentArray() {
    if (_segment.array().length == _segment.remaining()) {
      return _segment.array();
    } else {
      byte[] copy = new byte[_segment.remaining()];
      _segment.get(copy);
      return copy;
    }
  }

  int sequenceNumber() {
    return _sequenceNumber;
  }

  boolean originalKeyWasNull() {
    return _originalKeyWasNull;
  }
}
