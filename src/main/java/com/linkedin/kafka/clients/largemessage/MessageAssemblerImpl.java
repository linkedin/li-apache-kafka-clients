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

import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.consumer.HeaderKeySpace;
import java.nio.ByteBuffer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
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
    if (segmentRecord.header(HeaderKeySpace.LARGE_MESSAGE_SEGMENT_HEADER) == null) {
      throw new IllegalArgumentException("Not a large message segment.");
    }

    LargeMessageSegment segment =
      new LargeMessageSegment(segmentRecord.header(HeaderKeySpace.LARGE_MESSAGE_SEGMENT_HEADER), ByteBuffer.wrap(segmentRecord.value()));
      // Return immediately if it is a single segment message.
    if (segment.numberOfSegments() == 1) {
      return new AssembleResult(segment.segmentArray(), offset, offset, Collections.emptySet(), segment.originalKeyWasNull(), segmentRecord.headersSize());
    } else {
      LargeMessage.SegmentAddResult result = _messagePool.tryCompleteMessage(tp, offset, segment, segmentRecord.headersSize());
      return new AssembleResult(result.serializedMessage(), result.startingOffset(), offset, result.segmentOffsets(),
          segment.originalKeyWasNull(), result.totalHeadersSize());
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
