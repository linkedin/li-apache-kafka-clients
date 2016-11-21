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
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * The interface of the assembler on consumer side to re-assemeble the message segments created by
 * {@link MessageSplitter}. Message assembler is also responsible for keep tracking of the safe offset to commit
 * for a partition (see {@link #safeOffsets()})
 */
public interface MessageAssembler {

  /**
   * Assemble the message segments to the original value.
   * When the segment provided can complete an original value, the original value will be returned. Otherwise it
   * returns null.
   *
   * @param srcRecord a message segment in byte array format created by {@link MessageSplitter}
   *
   * @return The assemble result if a message is successfully assembled, otherwise returns null.
   */
  AssembleResult assemble(TopicPartition tp, long offset, ExtensibleConsumerRecord<byte[], byte[]> srcRecord);

  /**
   * This method should return the safe offset to commit for each partition.
   * A safe offset for a partition is the smallest of the first segment across all incomplete messages.
   * A safe offset will guarantee that all the segments of an incomplete message will be consumed again if
   * the consumer accidentally died.
   *
   * @return a map of safe offset for each partition.
   */
  Map<TopicPartition, Long> safeOffsets();

  /**
   * Get the safe offset for a particular partition. When safe offset of a partition is not available, Long.Max_Value
   * will be returned.
   *
   * @param tp the topic partition to get safe offset.
   * @return the safe offset.
   */
  long safeOffset(TopicPartition tp);

  /**
   * This method is to clean up all the states in the message assembler.
   */
  void clear();

  /**
   * This method clears up the states of a partition.
   *
   * @param tp the partition to clear state.
   */
  void clear(TopicPartition tp);

  /**
   * Close the assembler.
   */
  void close();

  /**
   * The completed, large message value; all segments concatenated into one.
   */
  static class AssembleResult {
    private final boolean _originalKeyIsNull;
    private final byte[] _messageBytes;
    private final long _messageStartingOffset;
    private final long _messageEndingOffset;
    private final int _totalHeadersSize;
    private final Set<Long> _segmentOffsets;

    AssembleResult(byte[] messageBytes, long startingOffset, long endingOffset, Set<Long> segmentOffsets, boolean originalKeyIsNull, int totalHeadersSize) {
      _messageBytes = messageBytes;
      _messageStartingOffset = startingOffset;
      _messageEndingOffset = endingOffset;
      _segmentOffsets = segmentOffsets;
      _originalKeyIsNull = originalKeyIsNull;
      _totalHeadersSize = totalHeadersSize;
    }

    public byte[] messageBytes() {
      return _messageBytes;
    }

    public long messageStartingOffset() {
      return _messageStartingOffset;
    }

    public long messageEndingOffset() {
      return _messageEndingOffset;
    }

    public Set<Long> segmentOffsets() {
      return _segmentOffsets;
    }

    public boolean isOriginalKeyIsNull() {
      return _originalKeyIsNull;
    }

    public int totalHeadersSize() {
      return _totalHeadersSize;
    }
  }
}
