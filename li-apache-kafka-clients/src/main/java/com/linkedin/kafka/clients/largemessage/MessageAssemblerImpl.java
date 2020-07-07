/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.common.LargeMessageHeaderValue;
import com.linkedin.kafka.clients.utils.Constants;
import java.nio.ByteBuffer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;

import static com.linkedin.kafka.clients.largemessage.MessageAssembler.AssembleResult.INCOMPLETE_RESULT;


/**
 * The implementation of {@link MessageAssembler}
 */
public class MessageAssemblerImpl implements MessageAssembler {
  private final LargeMessageBufferPool _messagePool;
  private final Deserializer<LargeMessageSegment> _segmentDeserializer;

  public MessageAssemblerImpl(long bufferCapacity,
                              long expirationOffsetGap,
                              boolean exceptionOnMessageDropped,
                              Deserializer<LargeMessageSegment> segmentDeserializer) {
    _messagePool = new LargeMessageBufferPool(bufferCapacity, expirationOffsetGap, exceptionOnMessageDropped);
    _segmentDeserializer = segmentDeserializer;
  }

  public MessageAssemblerImpl(long bufferCapacity,
                              long expirationOffsetGap,
                              boolean exceptionOnMessageDropped) {
    _messagePool = new LargeMessageBufferPool(bufferCapacity, expirationOffsetGap, exceptionOnMessageDropped);
    _segmentDeserializer = null;
  }

  @Deprecated
  public MessageAssemblerImpl(long bufferCapacity,
                              long expirationOffsetGap,
                              boolean exceptionOnMessageDropped,
                              Deserializer<LargeMessageSegment> segmentDeserializer,
                              @SuppressWarnings("unused") boolean treatInvalidMessageSegmentsAsPayload) {
    _messagePool = new LargeMessageBufferPool(bufferCapacity, expirationOffsetGap, exceptionOnMessageDropped);
    _segmentDeserializer = segmentDeserializer;
  }

  @Override
  public AssembleResult assemble(TopicPartition tp, long offset, byte[] segmentBytes) {
    if (segmentBytes == null) {
      return new AssembleResult(null, offset, offset);
    }

    LargeMessageSegment segment = _segmentDeserializer.deserialize(tp.topic(), segmentBytes);
    return assembleSegment(tp, offset, segmentBytes, segment);
  }

  @Override
  public AssembleResult assemble(TopicPartition tp,
                                 long offset,
                                 byte[] segmentBytes,
                                 Header header) {
    if (segmentBytes == null) {
      return new AssembleResult(null, offset, offset);
    }

    // no LM record header or wrong key, use default assemble()
    if (header == null || !header.key().equals(Constants.LARGE_MESSAGE_HEADER)) {
      return assemble(tp, offset, segmentBytes);
    }
    // retrieve segment header
    LargeMessageHeaderValue segmentHeader = LargeMessageHeaderValue.fromBytes(header.value());
    // check version, if it is older than V3, still use payload header solution
    if (segmentHeader.getType() < LargeMessageHeaderValue.V3) {
      return assemble(tp, offset, segmentBytes);
    }
    ByteBuffer payload = ByteBuffer.wrap(segmentBytes);

    // create segment
    LargeMessageSegment segment = new LargeMessageSegment(segmentHeader, payload);
    return assembleSegment(tp, offset, segmentBytes, segment);
  }

  @Override
  public long safeOffset(TopicPartition tp, long currentPosition) {
    return _messagePool.safeOffset(tp, currentPosition);
  }

  @Override
  public void clear() {
    _messagePool.clear();
  }

  @Override
  public void clear(TopicPartition tp) {
    _messagePool.clear(tp);
  }

  @Override
  public void close() {
  }

  // A helper method to avoid duplicate codes for assembling a segment
  private AssembleResult assembleSegment(TopicPartition tp,
                                         long offset,
                                         byte[] segmentBytes,
                                         LargeMessageSegment segment) {
    if (segment == null) {
      //not a segment
      return new AssembleResult(segmentBytes, offset, offset);
    } else {
      //sanity-check the segment
      segment.sanityCheck();

      // Return immediately if it is a single segment message.
      if (segment.numberOfSegments == 1) {
        return new AssembleResult(segment.payloadArray(), offset, offset);
      } else {
        LargeMessage.SegmentAddResult result = _messagePool.tryCompleteMessage(tp, offset, segment);
        return new AssembleResult(result.serializedMessage() == null ? INCOMPLETE_RESULT : result.serializedMessage(), result.startingOffset(), offset);
      }
    }
  }
}
