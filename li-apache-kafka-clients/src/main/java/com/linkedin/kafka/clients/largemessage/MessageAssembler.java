/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;


/**
 * The interface of the assembler on consumer side to re-assemeble the message segments created by
 * {@link MessageSplitter}. Message assembler is also responsible for keep tracking of the safe offset to commit
 * for a partition (see {@link #safeOffset})
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
  // TODO: Mark this method to be deprecated for the next major version bump.
  AssembleResult assemble(TopicPartition tp, long offset, byte[] segmentBytes);

  /**
   * Assemble the message segments to the original message.
   * When the segment provided can complete an original message, the original message will be returned. Otherwise it
   * returns null.
   *
   * @param tp the partition of this segment.
   * @param offset the offset of this segment.
   * @param segmentBytes a message segment in byte array format created by {@link MessageSplitter}
   * @param header record header of this segment
   * @return The assemble result if a message is successfully assembled, otherwise returns null.
   */
  AssembleResult assemble(TopicPartition tp, long offset, byte[] segmentBytes, Header header);
  /**
   * Get the safe offset for a particular partition. When safe offset of a partition is not available, Long.Max_Value
   * will be returned.  This will also expire any large messages that can not be assembled if the interval between
   * the uncompleted large message offset and the currentPosition is larger than some threshold for example
   * {@link com.linkedin.kafka.clients.consumer.LiKafkaConsumerConfig#MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_CONFIG}
   *
   * @param tp the topic partition to get safe offset.
   * @param currentPosition the current position of the consumer for the specified tp.  If the current position
   *                        is not known the highest known consumed position can be used with corresponding less
   *                        accuracy in expiring messages.
   * @return the safe offset.
   */
  long safeOffset(TopicPartition tp, long currentPosition);

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
