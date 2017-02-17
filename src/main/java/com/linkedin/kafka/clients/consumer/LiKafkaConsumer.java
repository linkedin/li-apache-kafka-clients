/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.annotations.InterfaceOrigin;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import com.linkedin.kafka.clients.largemessage.errors.OffsetNotTrackedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
   * Get the set of partitions currently assigned to this consumer. If subscription happened by directly assigning
   * partitions using {@link #assign(Collection)} then this will simply return the same partitions that
   * were assigned. If topic subscription was used, then this will give the set of topic partitions currently assigned
   * to the consumer (which may be none if the assignment hasn't happened yet, or the partitions are in the
   * process of getting reassigned).
   *
   * @return The set of partitions currently assigned to this consumer
   */
  @InterfaceOrigin.ApacheKafka
  Set<TopicPartition> assignment();

  /**
   * Get the current subscription. Will return the same topics used in the most recent call to
   * {@link #subscribe(Collection, ConsumerRebalanceListener)}, or an empty set if no such call has been made.
   *
   * @return The set of topics currently subscribed to
   */
  @InterfaceOrigin.ApacheKafka
  Set<String> subscription();

  /**
   * Subscribe to the given list of topics to get dynamically assigned partitions.
   * <b>Topic subscriptions are not incremental. This list will replace the current
   * assignment (if there is one).</b> It is not possible to combine topic subscription with group management
   * with manual partition assignment through {@link #assign(Collection)}.
   * <p>
   * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
   * <p>
   * This is a short-hand for {@link #subscribe(Collection, ConsumerRebalanceListener)}, which
   * uses a noop listener. If you need the ability to either seek to particular offsets, you should prefer
   * {@link #subscribe(Collection, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
   * to be reset. You should also prefer to provide your own listener if you are doing your own offset
   * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
   * <p>
   * In order to support large message, the consumer tracks all the consumed messages for each partition. When the
   * user no longer subscribes to a new set of topics, the consumer will discard all the tracked messages of the
   * partitions of that topic.
   *
   * @param topics The list of topics to subscribe to
   */
  @InterfaceOrigin.ApacheKafka
  void subscribe(Collection<String> topics);

  /**
   * Subscribe to the given list of topics to get dynamically
   * assigned partitions. <b>Topic subscriptions are not incremental. This list will replace the current
   * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group management
   * with manual partition assignment through {@link #assign(Collection)}.
   * <p>
   * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
   * <p>
   * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
   * group and will trigger a rebalance operation if one of the following events trigger -
   * <ul>
   * <li>Number of partitions change for any of the subscribed list of topics
   * <li>Topic is created or deleted
   * <li>An existing member of the consumer group dies
   * <li>A new member is added to an existing consumer group via the join API
   * </ul>
   * <p>
   * When any of these events are triggered, the provided listener will be invoked first to indicate that
   * the consumer's assignment has been revoked, and then again when the new assignment has been received.
   * Note that this listener will immediately override any listener set in a previous call to subscribe.
   * It is guaranteed, however, that the partitions revoked/assigned through this interface are from topics
   * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
   * <p>
   * In order to support large message, the consumer tracks all the consumed messages for each partition. When the
   * user no longer subscribes to a new set of topics, the consumer will discard all the tracked messages of the
   * partitions of that topic.
   *
   * @param topics   The list of topics to subscribe to
   * @param callback Non-null listener instance to get notifications on partition assignment/revocation for the
   *                 subscribed topics
   */
  @InterfaceOrigin.ApacheKafka
  void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

  /**
   * Manually assign a list of partition to this consumer. This interface does not allow for incremental assignment
   * and will replace the previous assignment (if there is one).
   * <p>
   * Manual topic assignment through this method does not use the consumer's group management
   * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
   * metadata change. Note that it is not possible to use both manual partition assignment with {@link #assign(Collection)}
   * and group assignment with {@link #subscribe(Collection, ConsumerRebalanceListener)}.
   * <p>
   * In order to support large message, the consumer tracks all the consumed messages for each partition. When the
   * the consumer is no longer assigned a partition, the consumer will discard all the tracked messages of the
   * partitions of that topic.
   *
   * @param partitions The list of partitions to assign this consumer
   */
  @InterfaceOrigin.ApacheKafka
  void assign(Collection<TopicPartition> partitions);

  /**
   * Subscribe to all topics matching specified pattern to get dynamically assigned partitions. The pattern matching will
   * be done periodically against topics existing at the time of check.
   * <p>
   * As part of group management, the consumer will keep track of the list of consumers that
   * belong to a particular group and will trigger a rebalance operation if one of the
   * following events trigger -
   * <ul>
   * <li>Number of partitions change for any of the subscribed list of topics
   * <li>Topic is created or deleted
   * <li>An existing member of the consumer group dies
   * <li>A new member is added to an existing consumer group via the join API
   * </ul>
   * <p>
   * In order to support large message, the consumer tracks all the consumed messages for each partition. When the
   * user no longer subscribes to a new set of topics, the consumer will discard all the tracked messages of the
   * partitions of that topic.
   *
   * @param pattern Pattern to subscribe to
   */
  @InterfaceOrigin.ApacheKafka
  void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

  /**
   * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)}. This
   * also clears any partitions directly assigned through {@link #assign(Collection)}.
   * <p>
   * In order to support large message, the consumer maintains some state internally for each partition. This method
   * will clear all the internal state maintained for large messages.
   */
  @InterfaceOrigin.ApacheKafka
  void unsubscribe();

  /**
   * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
   * subscribed to any topics or partitions before polling for data.
   * <p>
   * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
   * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
   * offset for the subscribed list of partitions
   *
   * @param timeout The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
   *                If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
   *                Must not be negative.
   * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
   * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
   *                                                                  partitions is undefined or out of range and no offset
   *                                                                  reset policy has been configured
   * @throws org.apache.kafka.common.errors.WakeupException           if {@link #wakeup()} is called before or while this
   *                                                                  function is called
   * @throws org.apache.kafka.common.errors.AuthorizationException    if caller does Read access to any of the subscribed
   *                                                                  topics or to the configured groupId
   * @throws org.apache.kafka.common.KafkaException                   for any other unrecoverable errors (e.g. invalid groupId or
   *                                                                  session timeout, errors deserializing key/value
   *                                                                  pairs, or any new error cases in future versions)
   */
  @InterfaceOrigin.ApacheKafka
  ConsumerRecords<K, V> poll(long timeout);

  /**
   * Commit offsets returned on the last {@link #poll(long) poll()} for all the subscribed list of topics and partitions.
   * <p>
   * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
   * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
   * should not be used.
   * <p>
   * This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is
   * encountered (in which case it is thrown to the caller).
   *
   * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
   *                                                                 This can only occur if you are using automatic group
   *                                                                 management with {@link #subscribe(Collection)},
   *                                                                 or if there is an active group with the same groupId
   *                                                                 which is using group management.
   * @throws org.apache.kafka.common.errors.WakeupException          if {@link #wakeup()} is called before or while this
   *                                                                 function is called
   * @throws org.apache.kafka.common.errors.AuthorizationException   if not authorized to the topic or to the
   *                                                                 configured groupId
   * @throws org.apache.kafka.common.KafkaException                  for any other unrecoverable errors (e.g. if offset metadata
   *                                                                 is too large or if the committed offset is invalid).
   */
  @InterfaceOrigin.ApacheKafka
  void commitSync();

  /**
   * Commit the specified offsets for the specified list of topics and partitions.
   * <p>
   * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
   * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
   * should not be used. The committed offset should be the next message your application will consume,
   * i.e. lastProcessedMessageOffset + 1.
   * <p>
   * This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is
   * encountered (in which case it is thrown to the caller).
   * <p>
   * This method is large message transparent.
   * <p>
   * With large message support, the consumer will internally translate the user provided offsets to the safe
   * offsets for the corresponding partitions (see {@link #safeOffset(TopicPartition, long)}. The actual offset
   * committed to Kafka can be retrieved through method {@link #committedSafeOffset(TopicPartition)}.
   *
   * @param offsets A map of offsets by partition with associated metadata
   * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
   *                                                                 This can only occur if you are using automatic group
   *                                                                 management with {@link #subscribe(Collection)},
   *                                                                 or if there is an active group with the same groupId
   *                                                                 which is using group management.
   * @throws org.apache.kafka.common.errors.WakeupException          if {@link #wakeup()} is called before or while this
   *                                                                 function is called
   * @throws org.apache.kafka.common.errors.AuthorizationException   if not authorized to the topic or to the
   *                                                                 configured groupId
   * @throws org.apache.kafka.common.KafkaException                  for any other unrecoverable errors (e.g. if offset metadata
   *                                                                 is too large or if the committed offset is invalid).
   */
  @InterfaceOrigin.ApacheKafka
  void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

  /**
   * Commit offsets returned on the last {@link #poll(long) poll()} for all the subscribed list of topics and partition.
   * Same as {@link #commitAsync(OffsetCommitCallback) commitAsync(null)}
   */
  @InterfaceOrigin.ApacheKafka
  void commitAsync();

  /**
   * Commit offsets returned on the last {@link #poll(long) poll()} for the subscribed list of topics and partitions.
   * <p>
   * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
   * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
   * should not be used.
   * <p>
   * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
   * (if provided) or discarded.
   *
   * @param callback Callback to invoke when the commit completes
   */
  @InterfaceOrigin.ApacheKafka
  void commitAsync(OffsetCommitCallback callback);

  /**
   * Commit the specified offsets for the specified list of topics and partitions to Kafka.
   * <p>
   * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
   * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
   * should not be used. The committed offset should be the next message your application will consume,
   * i.e. lastProcessedMessageOffset + 1.
   * <p>
   * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
   * (if provided) or discarded.
   * <p>
   * This method is large message transparent.
   * <p>
   * With large message support, the consumer will internally translate the user provided offsets to the safe
   * offsets for the corresponding partitions (see {@link #safeOffset(TopicPartition, long)}. The actual offset
   * committed to Kafka can be retrieved through method {@link #committedSafeOffset(TopicPartition)}. The arguments
   * passed to the callback is large message transparent.
   *
   * @param offsets  A map of offsets by partition with associate metadata. This map will be copied internally, so it
   *                 is safe to mutate the map after returning.
   * @param callback Callback to invoke when the commit completes
   */
  @InterfaceOrigin.ApacheKafka
  void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

  /**
   * Overrides the fetch offsets that the consumer will use on the next {@link #poll(long) poll(timeout)}. If this API
   * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
   * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
   * <p>
   * With large message support, the consumer maintains the internal state for the consumed message of each
   * partition. With the internal state, the consumer guarantees the messages after the offset sought to will
   * be redelivered. To reduce memory footprint, the consumer only tracks a configurable number of messages.
   * <p>
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
  @InterfaceOrigin.ApacheKafka
  void seek(TopicPartition partition, long offset);

  /**
   * Seek to the first offset for each of the given partitions. This function evaluates lazily, seeking to the
   * first offset in all partitions only when {@link #poll(long)} or {@link #position(TopicPartition)} are called.
   * If no partition is provided, seek to the first offset for all of the currently assigned partitions.
   * <p>
   * Because the broker is not large message aware, it is possible that when user seek to the beginning of the log
   * and start to consume, some of the large message segments has been expired on the broker side and some segments
   * are still available. Therefore it is not guaranteed that user will see the same sequence of messages when they
   * seek to the beginning of the log.
   */
  @InterfaceOrigin.ApacheKafka
  void seekToBeginning(Collection<TopicPartition> partitions);

  /**
   * Seek to the last offset for each of the given partitions. This function evaluates lazily, seeking to the
   * final offset in all partitions only when {@link #poll(long)} or {@link #position(TopicPartition)} are called.
   * If no partition is provided, seek to the final offset for all of the currently assigned partitions.
   */
  @InterfaceOrigin.ApacheKafka
  void seekToEnd(Collection<TopicPartition> partitions);

  /**
   * Seek to the committed offsets of this consumer group for the given partitions.
   * <p>
   * This method is large message aware. This method is synchronous and takes effect immediately. If there is
   * no offset committed for some of the partitions, an exception will be thrown.
   *
   * @param partitions The partitions to seek to committed offsets.
   * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException if no offset has been committed before.
   */
  @InterfaceOrigin.LiKafkaClients
  void seekToCommitted(Collection<TopicPartition> partitions);

  /**
   * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
   *
   * @param partition The partition to get the position for
   * @return The offset
   * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently defined for
   *                                                                  the partition
   * @throws org.apache.kafka.common.errors.WakeupException           if {@link #wakeup()} is called before or while this
   *                                                                  function is called
   * @throws org.apache.kafka.common.errors.AuthorizationException    if not authorized to the topic or to the
   *                                                                  configured groupId
   * @throws org.apache.kafka.common.KafkaException                   for any other unrecoverable errors
   */
  @InterfaceOrigin.ApacheKafka
  long position(TopicPartition partition);

  /**
   * Get the last committed offset for the given partition (whether the commit happened by this process or
   * another). This offset will be used as the position for the consumer in the event of a failure.
   * <p>
   * This call may block to do a remote call if the partition in question isn't assigned to this consumer or if the
   * consumer hasn't yet initialized its cache of committed offsets.
   *
   * @param partition The partition to check
   * @return The last committed offset and metadata or null if there was no prior commit
   * @throws org.apache.kafka.common.errors.WakeupException        if {@link #wakeup()} is called before or while this
   *                                                               function is called
   * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
   *                                                               configured groupId
   * @throws org.apache.kafka.common.KafkaException                for any other unrecoverable errors
   */
  @InterfaceOrigin.ApacheKafka
  OffsetAndMetadata committed(TopicPartition partition);

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
   * Get the metrics kept by the consumer
   */
  @InterfaceOrigin.ApacheKafka
  Map<MetricName, ? extends Metric> metrics();

  /**
   * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
   * does not already have any metadata about the given topic.
   *
   * @param topic The topic to get partition metadata for
   * @return The list of partitions
   * @throws org.apache.kafka.common.errors.WakeupException        if {@link #wakeup()} is called before or while this
   *                                                               function is called
   * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the specified topic
   * @throws org.apache.kafka.common.errors.TimeoutException       if the topic metadata could not be fetched before
   *                                                               expiration of the configured request timeout
   * @throws org.apache.kafka.common.KafkaException                for any other unrecoverable errors
   */
  @InterfaceOrigin.ApacheKafka
  List<PartitionInfo> partitionsFor(String topic);

  /**
   * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
   * remote call to the server.
   *
   * @return The map of topics and its partitions
   * @throws org.apache.kafka.common.errors.WakeupException  if {@link #wakeup()} is called before or while this
   *                                                         function is called
   * @throws org.apache.kafka.common.errors.TimeoutException if the topic metadata could not be fetched before
   *                                                         expiration of the configured request timeout
   * @throws org.apache.kafka.common.KafkaException          for any other unrecoverable errors
   */
  @InterfaceOrigin.ApacheKafka
  Map<String, List<PartitionInfo>> listTopics();

  /**
   * Get the set of partitions that were previously paused by a call to {@link #pause(Collection)}.
   *
   * @return The set of paused partitions
   */
  @InterfaceOrigin.ApacheKafka
  Set<TopicPartition> paused();

  /**
   * Suspend fetching from the requested partitions. Future calls to {@link #poll(long)} will not return
   * any records from these partitions until they have been resumed using {@link #resume(Collection)}.
   * Note that this method does not affect partition subscription. In particular, it does not cause a group
   * rebalance when automatic assignment is used.
   *
   * @param partitions The partitions which should be paused
   */
  @InterfaceOrigin.ApacheKafka
  void pause(Collection<TopicPartition> partitions);

  /**
   * Resume specified partitions which have been paused with {@link #pause(Collection)}. New calls to
   * {@link #poll(long)} will return records from these partitions if there are any to be fetched.
   * If the partitions were not previously paused, this method is a no-op.
   *
   * @param partitions The partitions which should be resumed
   */
  @InterfaceOrigin.ApacheKafka
  void resume(Collection<TopicPartition> partitions);

  /**
   * This method is to help the users that want to checkpoint the offsets outside of Kafka. It returns the safe offset
   * of the specified partition when the message with the specified offset in this partition was delivered. User
   * have to make sure the offset provided is a valid offset for a delivered message.
   * <p>
   * The safe offset of a message <b>M</b> is the largest offset that guarantees all the messages after <b>M</b> will
   * be consumed if user starts to consume from that offset.
   * <p>
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

  /**
   * Close the consumer, waiting indefinitely for any needed cleanup. If auto-commit is enabled, this
   * will commit the current offsets. Note that {@link #wakeup()} cannot be use to interrupt close.
   */
  @InterfaceOrigin.ApacheKafka
  void close();

  /**
   * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
   * The thread which is blocking in an operation will throw {@link org.apache.kafka.common.errors.WakeupException}.
   */
  @InterfaceOrigin.ApacheKafka
  void wakeup();
}
