/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.common.LargeMessageHeaderValue;
import com.linkedin.kafka.clients.largemessage.errors.InvalidSegmentException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * The class that holds a large message segment.
 * <p>
 * Each large message segment contains the following information:
 * <ul>
 *   <li><b>MessageId:</b> The message id of the original large message.</li>
 *   <li><b>SequenceNumber:</b> The sequence number of the segment.</li>
 *   <li><b>NumberOfSegments:</b> The total number of segments the original large message has.</li>
 *   <li><b>MessageSizeInBytes:</b> The size of the original large message in bytes.</li>
 *   <li><b>payload:</b> The payload ByteBuffer of the segment.</li>
 * </ul>
 *
 * Please notice that it is not guaranteed that the payload ByteBuffer has a dedicated underlying byte array. To
 * get a dedicated byte array representation of the payload, {@link #payloadArray()} method should be called.
 *
 */
public class LargeMessageSegment {
  public final UUID messageId;
  public final int sequenceNumber;
  public final int numberOfSegments;
  public final int messageSizeInBytes;
  public final ByteBuffer payload;
  // The segment information over head bytes when serialize.
  public static final int SEGMENT_INFO_OVERHEAD = 16 + Integer.BYTES + Integer.BYTES + Integer.BYTES;
  // TODO: migrate to V3 version(i.e. changing CURRENT_VERSION to V3) when bumping major version
  // In the new major version, we shall only uses record headers for LM support instead of using both payload header and record header
  public static final byte CURRENT_VERSION = LargeMessageHeaderValue.LEGACY;

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

  public void sanityCheck() throws InvalidSegmentException {
    if (messageId == null) {
      throw new InvalidSegmentException("Message Id can not be null");
    }
    if (messageSizeInBytes < 0) {
      throw new InvalidSegmentException("message size (" + messageSizeInBytes + ") should be >= 0");
    }
    if (payload == null) {
      throw new InvalidSegmentException("payload cannot be null");
    }
    //this tries to handle cases where payload has not been flipped/rewound
    long dataSize = payload.position() > 0 ? payload.position() : payload.limit();
    if (dataSize > messageSizeInBytes) {
      throw new InvalidSegmentException("segment size (" + dataSize + ") should not be larger than message size (" + messageSizeInBytes + ")");
    }
    if (numberOfSegments <= 0) {
      throw new InvalidSegmentException("number of segments should be > 0, instead is " + numberOfSegments);
    }
    if (sequenceNumber < 0 || sequenceNumber > numberOfSegments - 1) {
      throw new InvalidSegmentException("Sequence number " + sequenceNumber + " should fall between [0," + (numberOfSegments - 1) + "].");
    }
  }

  @Override
  public String toString() {
    return "[messageId=" + messageId + ",seq=" + sequenceNumber + ",numSegs=" + numberOfSegments + ",messageSize=" +
        messageSizeInBytes + ",payloadSize=" + payload.limit() + "]";
  }
}
