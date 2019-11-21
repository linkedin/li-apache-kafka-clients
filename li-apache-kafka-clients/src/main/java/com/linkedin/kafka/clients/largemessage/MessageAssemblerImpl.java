/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.largemessage.errors.InvalidSegmentException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.clients.largemessage.MessageAssembler.AssembleResult.INCOMPLETE_RESULT;


/**
 * The implementation of {@link MessageAssembler}
 */
public class MessageAssemblerImpl implements MessageAssembler {
  private static final Logger LOG = LoggerFactory.getLogger(MessageAssemblerImpl.class);
  private final LargeMessageBufferPool _messagePool;
  private final Deserializer<LargeMessageSegment> _segmentDeserializer;
  private final boolean _treatInvalidMessageSegmentsAsPayload;

  public MessageAssemblerImpl(long bufferCapacity,
                              long expirationOffsetGap,
                              boolean exceptionOnMessageDropped,
                              Deserializer<LargeMessageSegment> segmentDeserializer) {
    this(bufferCapacity, expirationOffsetGap, exceptionOnMessageDropped, segmentDeserializer, false);
  }

  public MessageAssemblerImpl(long bufferCapacity,
                              long expirationOffsetGap,
                              boolean exceptionOnMessageDropped,
                              Deserializer<LargeMessageSegment> segmentDeserializer,
                              boolean treatInvalidMessageSegmentsAsPayload) {
    _messagePool = new LargeMessageBufferPool(bufferCapacity, expirationOffsetGap, exceptionOnMessageDropped);
    _segmentDeserializer = segmentDeserializer;
    _treatInvalidMessageSegmentsAsPayload = treatInvalidMessageSegmentsAsPayload;
  }

  @Override
  public AssembleResult assemble(TopicPartition tp, long offset, byte[] segmentBytes) {
    if (segmentBytes == null) {
      return new AssembleResult(null, offset, offset);
    }

    LargeMessageSegment segment = _segmentDeserializer.deserialize(tp.topic(), segmentBytes);
    if (segment == null) {
      //not a segment
      return new AssembleResult(segmentBytes, offset, offset);
    } else {
      //sanity-check the segment
      try {
        segment.sanityCheck();
      } catch (InvalidSegmentException e) {
        if (_treatInvalidMessageSegmentsAsPayload) {
          //behave as if this didnt look like a segment to us
          LOG.warn("message at {}/{} may have been a false-positive segment and will be treated as regular payload", tp, offset, e);
          return new AssembleResult(segmentBytes, offset, offset);
        } else {
          throw e;
        }
      }
      // Return immediately if it is a single segment message.
      if (segment.numberOfSegments == 1) {
        return new AssembleResult(segment.payloadArray(), offset, offset);
      } else {
        LargeMessage.SegmentAddResult result = _messagePool.tryCompleteMessage(tp, offset, segment);
        return new AssembleResult(result.serializedMessage() == null ? INCOMPLETE_RESULT : result.serializedMessage(),
                                  result.startingOffset(), offset);
      }
    }
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

}
