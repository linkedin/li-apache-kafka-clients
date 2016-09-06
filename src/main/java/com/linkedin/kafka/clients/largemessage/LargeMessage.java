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
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * The class to buffer incomplete message segments.
 */
public class LargeMessage {
  private final Map<Integer, ByteBuffer> _segments;
  private final Set<Long> _segmentOffsets;
  private final int _messageSize;
  private final int _numberOfSegments;
  private final TopicPartition _tp;
  private final UUID _messageId;
  private final long _startingOffset;
  private long _bufferedBytes;

  LargeMessage(TopicPartition tp, UUID messageId, long startingOffset, int messageSize, int numberOfSegments) {
    _messageSize = messageSize;
    _numberOfSegments = numberOfSegments;
    _segments = new HashMap<>();
    _segmentOffsets = new HashSet<>();
    _bufferedBytes = 0;
    _tp = tp;
    _messageId = messageId;
    _startingOffset = startingOffset;
  }

  public synchronized long bufferedSizeInBytes() {
    return _bufferedBytes;
  }

  public synchronized SegmentAddResult addSegment(LargeMessageSegment segment, long offset) {
    int seq = segment.sequenceNumber;
    int segmentSize = segment.payload.remaining();
    validateSegment(segment);
    // Ignore duplicated segment.
    if (!_segments.containsKey(seq)) {
      _segments.put(seq, segment.payload);
      _bufferedBytes += segmentSize;
      if (_segments.size() == _numberOfSegments) {
        // If we have got all the segments, assemble the original serialized message.
        return new SegmentAddResult(assembleMessage(), segmentSize, _startingOffset, _segmentOffsets);
      }
      // Add the segment offsets. We only track the segment offsets except the last segment.
      _segmentOffsets.add(offset);
    } else {
      // duplicate segment
      return new SegmentAddResult(null, 0, _startingOffset, _segmentOffsets);
    }
    // The segment is buffered, but it did not complete a large message.
    return new SegmentAddResult(null, segmentSize, _startingOffset, _segmentOffsets);
  }

  public TopicPartition topicPartition() {
    return _tp;
  }

  public UUID messageId() {
    return _messageId;
  }

  public long startingOffset() {
    return _startingOffset;
  }

  public String toString() {
    return String.format("[TopicPartition:%s, UUID:%s, NumberOfSegments:%d, MessageSize:%d, BufferedBytes:%d]",
        _tp, _messageId, _numberOfSegments, _messageSize, _bufferedBytes);
  }

  private void validateSegment(LargeMessageSegment segment) {
    int segmentSize = segment.payload.remaining();
    int seq = segment.sequenceNumber;

    if (segmentSize <= 0) {
      throw new InvalidSegmentException("Invalid segment: " + segment + ". Segment size should be greater than 0.");
    }

    if (_messageSize != segment.messageSizeInBytes
        || _numberOfSegments != segment.numberOfSegments) {
      throw new InvalidSegmentException("Detected UUID conflict. Segment: " + segment);
    }

    if (!_segments.containsKey(seq) && _bufferedBytes + segmentSize > _messageSize) {
      throw new InvalidSegmentException("Invalid segment: " + segment + ". Segments have more bytes than the " +
          "message has. Message size =" + _messageSize + ", segments total bytes = " +
          (_bufferedBytes + segmentSize));
    }
  }

  private byte[] assembleMessage() {
    if (_bufferedBytes != _messageSize) {
      throw new InvalidSegmentException("Buffered bytes in the message should equal to message size."
          + " Buffered bytes = " + _bufferedBytes + "message size = " + _messageSize);
    }
    byte[] serializedMessage = new byte[_messageSize];
    int segmentStart = 0;
    for (int i = 0; i < _numberOfSegments; i++) {
      ByteBuffer payload = _segments.get(i);
      int payloadSize = payload.remaining();
      payload.get(serializedMessage, segmentStart, payloadSize);
      segmentStart += payloadSize;
    }
    assert (segmentStart == _messageSize);
    return serializedMessage;
  }

  /**
   * This is the container class to return the result of a segment addition.
   */
  class SegmentAddResult {
    private final byte[] _serializedMessage;
    private final long _startingOffset;
    private final int _bytesAdded;
    private final Set<Long> _segmentOffsets;

    SegmentAddResult(byte[] serializedMessage, int bytesAdded, long startingOffset, Set<Long> segmentOffsets) {
      _serializedMessage = serializedMessage;
      _bytesAdded = bytesAdded;
      _startingOffset = startingOffset;
      _segmentOffsets = segmentOffsets;
    }

    /**
     * Return the completed large message in its serialized bytes format.
     *
     * @return The assembled serialized message if a large message is completed, otherwise null.
     */
    byte[] serializedMessage() {
      return _serializedMessage;
    }

    /**
     * @return The size of segment in bytes that has been added to the buffer. It does not count for duplicate segment.
     */
    int bytesAdded() {
      return _bytesAdded;
    }

    /**
     * @return the offset of the first segment in this large message.
     */
    long startingOffset() {
      return _startingOffset;
    }

    /**
     * @return the segment offsets of this large message.
     */
    Set<Long> segmentOffsets() {
      return _segmentOffsets;
    }
  }

}