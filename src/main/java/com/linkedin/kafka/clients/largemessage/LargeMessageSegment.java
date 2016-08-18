/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

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
