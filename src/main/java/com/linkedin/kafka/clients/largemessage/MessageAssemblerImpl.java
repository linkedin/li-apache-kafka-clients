/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.headers.HeaderUtils;
import java.nio.ByteBuffer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The implementation of {@link MessageAssembler}
 */
public class MessageAssemblerImpl implements MessageAssembler {
  private static final Logger LOG = LoggerFactory.getLogger(MessageAssemblerImpl.class);
  private final LargeMessageBufferPool _messagePool;

  public MessageAssemblerImpl(long bufferCapacity,
                              long expirationOffsetGap,
                              boolean exceptionOnMessageDropped) {
    _messagePool = new LargeMessageBufferPool(bufferCapacity, expirationOffsetGap, exceptionOnMessageDropped);
  }

  @Override
  public AssembleResult assemble(TopicPartition tp, long offset, ExtensibleConsumerRecord<byte[], byte[]> segmentRecord) {
    if (segmentRecord.header(HeaderUtils.LARGE_MESSAGE_SEGMENT_HEADER) == null) {
      return null;
    }

    LargeMessageSegment segment =
      new LargeMessageSegment(segmentRecord.header(HeaderUtils.LARGE_MESSAGE_SEGMENT_HEADER), ByteBuffer.wrap(segmentRecord.value()));
      // Return immediately if it is a single segment message.
    if (segment.numberOfSegments() == 1) {
      return new AssembleResult(segment.segmentArray(), offset, offset, segment.originalKeyWasNull());
    } else {
      LargeMessage.SegmentAddResult result = _messagePool.tryCompleteMessage(tp, offset, segment);
      return new AssembleResult(result.serializedMessage(), result.startingOffset(), offset,
          segment.originalKeyWasNull());
    }
  }

  @Override
  public Map<TopicPartition, Long> safeOffsets() {
    return _messagePool.safeOffsets();
  }

  @Override
  public long safeOffset(TopicPartition tp) {
    return _messagePool.safeOffset(tp);
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
