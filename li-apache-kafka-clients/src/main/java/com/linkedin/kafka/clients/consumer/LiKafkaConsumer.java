/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.annotations.InterfaceOrigin;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import com.linkedin.kafka.clients.largemessage.errors.OffsetNotTrackedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * LiKafkaConsumer is a consumer built on top of {@link Consumer}. LiKafkaConsumer has built-in support for large
 * messages and auditing.
 * <p>
 * A large message is a message which consists of multiple message segments (see {@link LargeMessageSegment}). The
 * message segments are Kafka messages with their own offsets on the broker. LiKafkaClients supports large messages
 * by reassembling the message segments that belong to the same large message.
 * <p>
 * The offset of a large message is the offset of its last segment. We made this design decision so that the consumers
 * can deliver a large message as soon as all the message segments of that large message have been consumed.
 * <p>
 * With the introduction of the support for large messages, we added a few new methods besides the existing methods in
 * {@link Consumer} to help user better manage the offset with the existence of large messages. The added
 * methods are:
 * <pre>
 *   {@code
 *   long committedSafeOffset(TopicPartition partition);
 *   void seekToCommitted(Collection<TopicPartition> partitions);
 *   long safeOffset(TopicPartition tp, long messageOffset);
 *   long safeOffset(TopicPartition tp);
 *   Map&lt;TopicPartition, Long&gt; safeOffsets();
 *   }
 * </pre>
 * In most cases, large messages are transparent to the users. However, when the user wants to explicitly handle
 * offsets of messages (e.g. providing offsets to or retrieving offsets from LiKafkaConsumer), the above methods may
 * be handy. Please read the documentation of the corresponding method to ensure correct usage.
 */
public interface LiKafkaConsumer<K, V> extends Consumer<K, V> {

  /**
   * {@inheritDoc}
   * <p>
   * In order to support large message, the consumer tracks all the consumed messages for each partition. When the
   * user no longer subscribes to a new set of topics, the consumer will discard all the tracked messages of the
   * partitions of that topic.
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void subscribe(Collection<String> topics);

  /**
   * {@inheritDoc}
   * <p>
   * In order to support large message, the consumer tracks all the consumed messages for each partition. When the
   * user no longer subscribes to a new set of topics, the consumer will discard all the tracked messages of the
   * partitions of that topic.
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

  /**
   * {@inheritDoc}
   * <p>
   * In order to support large message, the consumer tracks all the consumed messages for each partition. When the
   * the consumer is no longer assigned a partition, the consumer will discard all the tracked messages of the
   * partitions of that topic.
   *
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

  /**
   * {@inheritDoc}
   * <p>
   * In order to support large message, the consumer tracks all the consumed messages for each partition. When the
   * the consumer is no longer assigned a partition, the consumer will discard all the tracked messages of the
   * partitions of that topic.
   *
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void subscribe(Pattern pattern);

  /**
   * {@inheritDoc}
   * <p>
   * In order to support large message, the consumer maintains some state internally for each partition. This method
   * will clear all the internal state maintained for large messages.
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void unsubscribe();

  /**
   * {@inheritDoc}
   * <p>
   * In order to support large message, the consumer tracks all the consumed messages for each partition. When the
   * the consumer is no longer assigned a partition, the consumer will discard all the tracked messages of the
   * partitions of that topic.
   *
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void assign(Collection<TopicPartition> partitions);

  /**
   * {@inheritDoc}
   * <p>
   * With large message support, the consumer will internally translate the user provided offsets to the safe
   * offsets for the corresponding partitions (see {@link #safeOffset(TopicPartition, long)}. The actual offset
   * committed to Kafka can be retrieved through method {@link #committedSafeOffset(TopicPartition)}.
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

  /**
   * {@inheritDoc}
   * <p>
   * With large message support, the consumer will internally translate the user provided offsets to the safe
   * offsets for the corresponding partitions (see {@link #safeOffset(TopicPartition, long)}. The actual offset
   * committed to Kafka can be retrieved through method {@link #committedSafeOffset(TopicPartition)}.
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout);

  /**
   * {@inheritDoc}
   * <p>
   * This method is large message transparent.
   * <p>
   * With large message support, the consumer will internally translate the user provided offsets to the safe
   * offsets for the corresponding partitions (see {@link #safeOffset(TopicPartition, long)}. The actual offset
   * committed to Kafka can be retrieved through method {@link #committedSafeOffset(TopicPartition)}. The arguments
   * passed to the callback is large message transparent.
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

  /**
   * {@inheritDoc}
   * <p>
   * With large message support, the consumer maintains the internal state for the consumed message of each
   * partition. With the internal state, the consumer guarantees the messages after the offset sought to will
   * be redelivered. To reduce memory footprint, the consumer only tracks a configurable number of messages.
   * <p>
   * When this method is invoked, the behavior of this method is following:
   * <ul>
   * <li> If no message has ever been consumed from the partition, the consumer will seek to the user passed in
   * offset.
   * <li> If some messages have been consumed from the partition and user is seeking forward, i.e. the offset
   * argument is greater than the last consumed offset. The consumer will seek to the user passed in offset.
   * <li> If some messages have been consumed from the partition and user is seeking backward, i.e. the offset
   * argument is less than or equals to the last consumed offset, the consumer will seek to the safe offset
   * (see {@link #safeOffset(TopicPartition, long)}) of the specified message.
   * <li> If the offset user is seeking to is smaller than the earliest offset that the consumer was keeping
   * track of, an {@link OffsetNotTrackedException} will be thrown.
   * </ul>
   * When this method returned without throwing exception, the internally tracked offsets for this partition are
   * discarded.
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void seek(TopicPartition partition, long offset);

  /**
   * {@inheritDoc}
   * <p>
   * Because the broker is not large message aware, it is possible that when user seek to the beginning of the log
   * and start to consume, some of the large message segments has been expired on the broker side and some segments
   * are still available. Therefore it is not guaranteed that user will see the same sequence of messages when they
   * seek to the beginning of the log.
   */
  @Override
  @InterfaceOrigin.ApacheKafka
  void seekToBeginning(Collection<TopicPartition> partitions);

  /**
   * Seek to the committed offsets of this consumer group for the given partitions.
   * <p>
   * This method is large message aware. This method is synchronous and takes effect immediately. If there is
   * no offset committed for some of the partitions, an exception will be thrown.
   * @param partitions partitions for which to seek to committed offsets
   */
  @InterfaceOrigin.LiKafkaClients
  void seekToCommitted(Collection<TopicPartition> partitions);

  /**
   * With large message support, the consumer makes sure the offset committed to Kafka are the safe offsets
   * (see {@link #safeOffset(TopicPartition, long)}) to ensure no message loss. The safe offsets committed may be
   * different from the offsets that is provided by the users.
   * <p>
   * While the {@link #committed(TopicPartition)} method returns the message offset committed by user, this method
   * returns the safe offset committed by the consumer for the specified partition.
   *
   * @param partition The partition to query the committed offset.
   * @return The actual offset committed to Kafka for this partition, or -1 is returned if there is no committed
   * safe offset.
   */
  @InterfaceOrigin.LiKafkaClients
  Long committedSafeOffset(TopicPartition partition);

  /**
   * This method is to help the users that want to checkpoint the offsets outside of Kafka. It returns the safe offset
   * of the specified partition when the message with the specified offset in this partition was delivered. User
   * have to make sure the offset provided is a valid offset for a delivered message.
   * <p>
   * The safe offset of a message <b>M</b> is the largest offset that guarantees all the messages after <b>M</b> will
   * be consumed if user starts to consume from that offset.
   * <p>
   * For example, consider the following message/segment sequence:
   * <ul>
   * <li>offset 0 ----&gt; message0_segment0
   * <li>offset 1 ----&gt; message1
   * <li>offset 2 ----&gt; message0_segment1
   * </ul>
   * The offset of a large message is the offset of its last segment. In the above example the user will see
   * message 1 with offset 1 followed by message 0 with offset 2.
   * <p>
   * When safeOffset(tp, 0) is called, an {@link com.linkedin.kafka.clients.largemessage.errors.OffsetNotTrackedException} will
   * be thrown because 0 is not a valid offset of a message - the offset of a large message is the offset
   * of its last segment, which is 2 for message0.
   * <p>
   * When safeOffset(tp, 1) is called, 0 will be returned because when message 1 was delivered, message 0 is not
   * completed yet and the offset of its first segment is 0. In this case when user resume consumption from the safe
   * offset 0, message 1 will be delivered again.
   * <p>
   * When safeOffset(tp, 2) is called, 3 will be returned because there is no message depending on any earlier offsets
   * to be delivered after message 0. In this case, message 0 will not be consumed again if user starts to consume
   * from safe offset 3.
   * <p>
   * <b>Note:</b> LiKafkaConsumer will avoid consuming duplicate messages when users let the consumer to manage the
   * offsets. More specifically, LiKafkaConsumer will filter out the duplicate messages when:
   * <ul>
   * <li>User uses Kafka group management, or, </li>
   * <li>User uses {@link #commitSync()}, {@link #commitAsync()} and {@link #seekToCommitted(Collection)}</li>
   * </ul>
   *
   * @param tp            The topic partition to get safe offset
   * @param messageOffset the offset of a delivered message
   * @return The safe offset when the specified message is delivered. null if no message has been consumed from the partition.
   */
  @InterfaceOrigin.LiKafkaClients
  Long safeOffset(TopicPartition tp, long messageOffset);

  /**
   * This method returns the current safe offset to commit for a specified partition.
   * <p>
   * A safe offset for a partition is the smallest of the first segment across all incomplete large messages in the
   * same partition.
   * A safe offset will guarantee that all the segments of all incomplete messages will be consumed again if
   * the consumer accidentally dies.
   * If the safe offset of a partition is not available - no message has been delivered from that partition - the
   * safe offset of the partition will be null.
   *
   * @param tp The partition to get safe offset.
   * @return Safe offset for the partition. null is returned if no message has been delivered from the given
   * partition.
   *
   * @see LiKafkaConsumer#safeOffset(TopicPartition, long)
   */
  @InterfaceOrigin.LiKafkaClients
  Long safeOffset(TopicPartition tp);

  /**
   * This method returns the safe offset to commit for each partition.
   * <p>
   * A safe offset for a partition is the smallest of the first segment across all incomplete large messages in the
   * same partition.
   * A safe offset guarantees that all the segments of an incomplete message will be consumed again if
   * the consumer accidentally died.
   *
   * @return a map of safe offset for each partition.
   *
   * @see LiKafkaConsumer#safeOffset(TopicPartition, long)
   */
  @InterfaceOrigin.LiKafkaClients
  Map<TopicPartition, Long> safeOffsets();
}
