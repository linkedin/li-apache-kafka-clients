/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * The interface of the assembler on consumer side to re-assemeble the message segments created by
 * {@link MessageSplitter}. Message assembler is also responsible for keep tracking of the safe offset to commit
 * for a partition (see {@link #safeOffsets()})
 */
public interface MessageAssembler {

  /**
   * Assemble the message segments to the original message.
   * When the segment provided can complete an original message, the original message will be returned. Otherwise it
   * returns null.
   *
   * @param tp the partition of this segment.
   * @param offset the offset of this segment.
   * @param segmentBytes a message segment in byte array format created by {@link MessageSplitter}
   * @return The assemble result if a message is successfully assembled, otherwise returns null.
   */
  AssembleResult assemble(TopicPartition tp, long offset, byte[] segmentBytes);

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

  class AssembleResult {
    public static final byte[] INCOMPLETE_RESULT = new byte[0];
    private final byte[] _messageBytes;
    private final long _messageStartingOffset;
    private final long _messageEndingOffset;

    AssembleResult(byte[] messageBytes, long startingOffset, long endingOffset) {
      _messageBytes = messageBytes;
      _messageStartingOffset = startingOffset;
      _messageEndingOffset = endingOffset;
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

  }
}
