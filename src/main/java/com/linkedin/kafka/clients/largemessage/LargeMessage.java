/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.largemessage.errors.InvalidSegmentException;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The class to buffer incomplete message segments.
 */
public class LargeMessage {
  private final Map<Integer, ByteBuffer> _segments;
  private final int _originalValueSize;

  private final int _numberOfSegments;
  private final TopicPartition _tp;
  private final UUID _messageId;
  private final long _startingOffset;
  private long _bufferedBytes;

  LargeMessage(TopicPartition tp, UUID messageId, long startingOffset, int originalValueSize, int numberOfSegments) {
    _originalValueSize = originalValueSize;
    _numberOfSegments = numberOfSegments;
    _segments = new HashMap<>();
    _bufferedBytes = 0;
    _tp = tp;
    _messageId = messageId;
    _startingOffset = startingOffset;
  }

  public synchronized long bufferedSizeInBytes() {
    return _bufferedBytes;
  }

  public synchronized SegmentAddResult addSegment(LargeMessageSegment segment, long offset) {
    int seq = segment.sequenceNumber();
    int segmentSize = segment.segmentByteBuffer().remaining();
    validateSegment(segment);
    // Ignore duplicated segment.
    if (!_segments.containsKey(seq)) {
      _segments.put(seq, segment.segmentByteBuffer());
      _bufferedBytes += segmentSize;
      if (_segments.size() == _numberOfSegments) {
        // If we have got all the segments, assemble the original serialized message.
        return new SegmentAddResult(assembleMessage(), segmentSize, _startingOffset);
      }
    } else {
      // duplicate segment
      return new SegmentAddResult(null, 0, _startingOffset);
    }
    // The segment is buffered, but it did not complete a large message.
    return new SegmentAddResult(null, segmentSize, _startingOffset);
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
        _tp, _messageId, _numberOfSegments, _originalValueSize, _bufferedBytes);
  }

  private void validateSegment(LargeMessageSegment segment) {
    int segmentSize = segment.segmentByteBuffer().remaining();
    int seq = segment.sequenceNumber();

    if (segmentSize <= 0) {
      throw new InvalidSegmentException("Invalid segment: " + segment + ". Segment size should be greater than 0.");
    }

    if (_originalValueSize != segment.originalValueSize()
        || _numberOfSegments != segment.numberOfSegments()) {
      throw new InvalidSegmentException("Segment number of offsets does not equal the known number of offsets for segment: " + segment);
    }

    if (!_segments.containsKey(seq) && _bufferedBytes + segmentSize > _originalValueSize) {
      throw new InvalidSegmentException("Invalid segment: " + segment + ". Segments have more bytes than the " +
          "message has. Message size =" + _originalValueSize + ", segments total bytes = " +
          (_bufferedBytes + segmentSize));
    }
  }

  private byte[] assembleMessage() {
    if (_bufferedBytes != _originalValueSize) {
      throw new InvalidSegmentException("Buffered bytes in the message should equal to message size."
          + " Buffered bytes = " + _bufferedBytes + "message size = " + _originalValueSize);
    }
    byte[] serializedMessage = new byte[_originalValueSize];
    int segmentStart = 0;
    for (int i = 0; i < _numberOfSegments; i++) {
      ByteBuffer payload = _segments.get(i);
      int payloadSize = payload.remaining();
      payload.get(serializedMessage, segmentStart, payloadSize);
      segmentStart += payloadSize;
    }
    if (segmentStart !=  _originalValueSize) {
      throw new IllegalStateException("segmentStart " + segmentStart + " != originalValueSize " + _originalValueSize);
    }
    return serializedMessage;
  }

  /**
   * This is the container class to return the result of a segment addition.
   */
  class SegmentAddResult {
    private final byte[] _serializedMessage;
    private final long _startingOffset;
    private final int _bytesAdded;

    SegmentAddResult(byte[] serializedMessage, int bytesAdded, long startingOffset) {
      _serializedMessage = serializedMessage;
      _bytesAdded = bytesAdded;
      _startingOffset = startingOffset;
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
  }

}