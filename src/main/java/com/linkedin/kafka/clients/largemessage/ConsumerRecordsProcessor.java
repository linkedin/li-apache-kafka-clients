/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class processes consumer records returned by {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(long)}
 */
public class ConsumerRecordsProcessor<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerRecordsProcessor.class);
  private final MessageAssembler _messageAssembler;
  private final Deserializer<K> _keyDeserializer;
  private final Deserializer<V> _valueDeserializer;
  private final DeliveredMessageOffsetTracker _deliveredMessageOffsetTracker;
  private final Map<TopicPartition, Long> _partitionConsumerHighWatermark;
  private final Auditor<K, V> _auditor;

  public ConsumerRecordsProcessor(MessageAssembler messageAssembler,
                                  Deserializer<K> keyDeserializer,
                                  Deserializer<V> valueDeserializer,
                                  DeliveredMessageOffsetTracker deliveredMessageOffsetTracker,
                                  Auditor<K, V> auditor) {
    _messageAssembler = messageAssembler;
    _keyDeserializer = keyDeserializer;
    _valueDeserializer = valueDeserializer;
    _deliveredMessageOffsetTracker = deliveredMessageOffsetTracker;
    _auditor = auditor;
    _partitionConsumerHighWatermark = new HashMap<>();
    if (_auditor == null) {
      LOG.info("Auditing is disabled because no auditor is defined.");
    }
  }

  /**
   * This method filters out the incomplete message segment records.
   *
   * @param consumerRecords The consumer records to be filtered.
   * @return filtered consumer records.
   */
  public ConsumerRecords<K, V> process(ConsumerRecords<byte[], byte[]> consumerRecords) {
    Map<TopicPartition, List<ConsumerRecord<K, V>>> filteredRecords = new HashMap<>();
    for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
      TopicPartition tp = new TopicPartition(record.topic(), record.partition());
      ConsumerRecord<K, V> handledRecord = handleConsumerRecord(record);
      // Only put record into map if it is not null
      if (handledRecord != null) {
        List<ConsumerRecord<K, V>> list = filteredRecords.get(tp);
        if (list == null) {
          list = new ArrayList<>();
          filteredRecords.put(tp, list);
        }
        list.add(handledRecord);
      }
    }
    return new ConsumerRecords<>(filteredRecords);
  }

  /**
   * This method returns the current safe offset to commit for a specified partition.
   * <p>
   * A safe offset for a partition is the smallest of the first segment across all incomplete large messages in the
   * same partition.
   * A safe offset will guarantee that all the segments of all incomplete messages will be consumed again if
   * the consumer accidentally dies.
   * If the safe offset of a partition is not available - no message has been delivered from that partition - the
   * safe offset of the partition will be Long.MAX_VALUE.
   *
   * @param tp The partition to get safe offset.
   * @return safe offset for the partition.
   */
  public Long safeOffset(TopicPartition tp) {
    return _deliveredMessageOffsetTracker.safeOffset(tp);
  }

  /**
   * This method returns the safe offset of the specified partition when message with a specific offset in this
   * partition was delivered. User have to make sure the offset provided is a valid offset for a delivered message.
   * <p>
   * The safe offset will guarantee all the messages delivered after the given delivered offset will be delivered
   * again if user resume consumption from there. User may or may not see the message with given offset again if
   * they start to consume from the safe offset.
   * <p>
   * For example, consider the following message/segment sequence:
   * <ul>
   * <li>offset 0 ----&gt; message0_segment0
   * <li>offset 1 ----&gt; message1
   * <li>offset 2 ----&gt; message0_segment1
   * </ul>
   * <p>
   * When safeOffset(tp, 0) is called, a {@link com.linkedin.kafka.clients.largemessage.errors.OffsetNotTrackedException} will
   * be thrown because 0 is not a valid offset of delivered message - the offset of a large message is the offset
   * of its last segment, which is 2 for message0.
   * <p>
   * When safeOffset(tp, 1) is called, 0 will be returned because when message 1 was delivered, message 0 is not
   * completed yet and the offset of its first segment is 0. In this case when user resume consumption from safe
   * offset 0, message 1 will be delivered again.
   * <p>
   * When safeOffset(tp, 2) is called, 3 will be returned because there is no message depending on any earlier offsets
   * to be delivered after message 0. In this case, message 0 will not be consumed again if user starts to consume
   * from safe offset 3.
   *
   * @param tp            The topic partition to get safe offset
   * @param messageOffset the offset of a delivered message
   * @return The safe offset when the specified message is delivered.
   */
  public Long safeOffset(TopicPartition tp, long messageOffset) {
    return _deliveredMessageOffsetTracker.safeOffset(tp, messageOffset);
  }

  /**
   * This method returns the safe offset to commit for each partition.
   * <p>
   * A safe offset for a partition is the smallest of the first segment across all incomplete large messages.
   * A safe offset will guarantee that all the segments of an incomplete message will be consumed again if
   * the consumer accidentally died.
   *
   * @return a mapping from partitions to safe offsets.
   */
  public Map<TopicPartition, OffsetAndMetadata> safeOffsetsToCommit() {
    Map<TopicPartition, OffsetAndMetadata> safeOffsetsToCommit = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : _deliveredMessageOffsetTracker.safeOffsets().entrySet()) {
      safeOffsetsToCommit.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
    }
    return safeOffsetsToCommit;
  }

  /**
   * This method takes an offset map that user attempts to commit and return the safe offsets to commit for each
   * partition in the provided map.
   * The safe offset will guarantee all the message delivered after the offset user attempted to commit will be
   * reconsumed if user restart consumption.
   * The offsets in the offsetsToCommit map must be offsets of delivered messages, otherwise LargeMessageException
   * might be thrown.
   *
   * @param offsetsToCommit the offset map user attempting commit.
   * @return the safe offset map that user should use to commit offsets.
   */
  public Map<TopicPartition, OffsetAndMetadata> safeOffsetsToCommit(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
                                                                    boolean ignoreConsumerHighWatermark) {
    Map<TopicPartition, OffsetAndMetadata> safeOffsetsToCommit = new HashMap<>();
    for (TopicPartition tp : offsetsToCommit.keySet()) {
      long origOffset = offsetsToCommit.get(tp).offset();
      Long safeOffsetToCommit = origOffset;
      if (origOffset > 0) {
        // We need to find the previous delivered offset from this partition and use its safe offset.
        Long previousDeliveredOffset = _deliveredMessageOffsetTracker.closestDeliveredUpTo(tp, origOffset - 1);
        if (previousDeliveredOffset != null) {
          safeOffsetToCommit = _deliveredMessageOffsetTracker.safeOffset(tp, previousDeliveredOffset);
        } else {
          safeOffsetToCommit = _deliveredMessageOffsetTracker.earliestTrackedOffset(tp);
          if (safeOffsetToCommit == null) {
            safeOffsetToCommit = origOffset;
          }
        }
      }
      // We need to combine the metadata with the high watermark. High watermark should never rewind.
      Long hw = _partitionConsumerHighWatermark.get(tp);
      hw = (hw == null || ignoreConsumerHighWatermark) ? origOffset : Math.max(origOffset, hw);
      String wrappedMetadata = LiKafkaClientsUtils.wrapMetadataWithOffset(offsetsToCommit.get(tp).metadata(), hw);
      OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(Math.min(safeOffsetToCommit, origOffset), wrappedMetadata);
      safeOffsetsToCommit.put(tp, offsetAndMetadata);
    }
    return safeOffsetsToCommit;
  }

  /**
   * This method will return the offset of the first segment of a large message. If the message offset is a normal
   * message, the message offset will be returned.
   *
   * @param tp            the topic partition of the message.
   * @param messageOffset the offset of the message.
   * @return the offset of the first segment of a large message.
   */
  public long startingOffset(TopicPartition tp, long messageOffset) {
    return _deliveredMessageOffsetTracker.startingOffset(tp, messageOffset);
  }

  /**
   * Get the earliest tracked offset for a partition.
   * @param tp The given partition.
   * @return the earliest tracked offset.
   */
  public Long earliestTrackedOffset(TopicPartition tp) {
    return _deliveredMessageOffsetTracker.earliestTrackedOffset(tp);
  }

  /**
   * This method will return the most recently delivered message whose offset is smaller or equals to the specified
   * offset.
   * The delivered messages are the messages returned to the users.
   *
   * @param tp            The partition to get the most recently delivered offset.
   * @param messageOffset the specified offset
   * @return the most recently delivered message whose offset is smaller or equals to the specified offset.
   * Returns null if there is no message that has been delivered from this partition.
   */
  public Long closestDeliveredUpTo(TopicPartition tp, long messageOffset) {
    return _deliveredMessageOffsetTracker.closestDeliveredUpTo(tp, messageOffset);
  }

  /**
   * Return the offset of the last delivered message of a partition.
   * The delivered messages are the messages returned to the users.
   *
   * @param tp the partition
   * @return the offset of the last delivered message from the given partition.
   * Returns null when no message has been delivered.
   */
  public Long delivered(TopicPartition tp) {
    return _deliveredMessageOffsetTracker.delivered(tp);
  }

  /**
   * Return the offsets of the delivered messages for all partitions.
   * The delivered messages are the messages returned to the users.
   *
   * @return the offsets of the last delivered messages for all the partitions.
   */
  public Map<TopicPartition, Long> delivered() {
    return _deliveredMessageOffsetTracker.delivered();
  }

  /**
   * Mark the high watermark for a partition. The consumer record processor will ignore the messages whose
   * offset is less than the high watermark. This is useful to avoid duplicates after a seek() or consumer
   * rebalance. Notice that the high watermark is not guaranteed to be last committed offset. It is only used
   * to explicitly specify the minimum acceptable message
   * <p>
   * Note: This the offset in seek won't be cleaned up if there is an automatic offset reset. User needs to
   * handle that by themselves.
   *
   * @param tp     the partition that seek() is called on.
   * @param offset the offset sought to.
   */
  public void setPartitionConsumerHighWaterMark(TopicPartition tp, long offset) {
    // When user seek to an offset, the HW should be that offset - 1.
    _partitionConsumerHighWatermark.put(tp, offset);
  }

  /**
   * Clear all the high watermarks tracked by the consumer record processor.
   */
  public void clearAllConsumerHighWaterMarks() {
    _partitionConsumerHighWatermark.clear();
  }

  /**
   * @return The number of high watermarks in track.
   */
  public int numConsumerHighWaterMarks() {
    return _partitionConsumerHighWatermark.size();
  }

  /**
   * Get the high watermark of a given partition.
   *
   * @param tp the partition to get high watermark.
   * @return the high watermark of the given partition.
   */
  public Long consumerHighWaterMarkForPartition(TopicPartition tp) {
    return _partitionConsumerHighWatermark.get(tp);
  }

  /**
   * This method cleans up all the state in the consumer record processor. It is useful when consumer rebalance
   * occurs.
   */
  public void clear() {
    _deliveredMessageOffsetTracker.clear();
    _messageAssembler.clear();
    _partitionConsumerHighWatermark.clear();
  }

  /**
   * This method clears up the state of a partition. It is useful when consumer seek is called.
   *
   * @param tp partition whose state needs to be cleared.
   */
  public void clear(TopicPartition tp) {
    _deliveredMessageOffsetTracker.clear(tp);
    _messageAssembler.clear(tp);
    _partitionConsumerHighWatermark.remove(tp);
  }

  public void close() {
    _messageAssembler.close();
    _auditor.close();
  }

  private ConsumerRecord<K, V> handleConsumerRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
    TopicPartition tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
    ConsumerRecord<K, V> handledRecord = null;
    K key = _keyDeserializer.deserialize(tp.topic(), consumerRecord.key());
    byte[] valueBytes = parseAndMaybeTrackRecord(tp, consumerRecord.offset(), consumerRecord.value());
    V value = _valueDeserializer.deserialize(tp.topic(), valueBytes);
    if (value != null) {
      if (_auditor != null) {
        _auditor.record(tp.topic(), key, value, consumerRecord.timestamp(), 1L,
                        (long) consumerRecord.value().length, AuditType.SUCCESS);
      }
      handledRecord = new ConsumerRecord<>(
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          consumerRecord.timestamp(),
          consumerRecord.timestampType(),
          consumerRecord.checksum(),
          consumerRecord.serializedKeySize(),
          valueBytes.length,
          _keyDeserializer.deserialize(consumerRecord.topic(), consumerRecord.key()),
          value);
    }
    return handledRecord;
  }

  private byte[] parseAndMaybeTrackRecord(TopicPartition tp, long messageOffset, byte[] bytes) {
    MessageAssembler.AssembleResult assembledResult = _messageAssembler.assemble(tp, messageOffset, bytes);
    if (assembledResult.messageBytes() != null) {
      LOG.trace("Got message {} from partition {}", messageOffset, tp);
      boolean shouldSkip = shouldSkip(tp, messageOffset);
      // The safe offset is the smaller one of the current message offset + 1 and current safe offset.
      long safeOffset = Math.min(messageOffset + 1, _messageAssembler.safeOffset(tp));
      _deliveredMessageOffsetTracker.track(tp,
                                           messageOffset,
                                           safeOffset,
                                           assembledResult.messageStartingOffset(),
                                           !shouldSkip);
      // We skip the messages whose offset is smaller than the high watermark.
      if (shouldSkip) {
        LOG.trace("Skipping message {} from partition {} because its offset is smaller than the high watermark",
                  messageOffset, tp);
        return null;
      } else {
        return assembledResult.messageBytes();
      }
    } else {
      _deliveredMessageOffsetTracker.addNonMessageOffset(tp, messageOffset);
      return null;
    }
  }

  /**
   * Check whether the message should be ignored.
   * The message should be skipped in the following cases:
   * 1. The offset is smaller than the offset in seek on the partition.
   * 2. The offset is smaller than the last delivered offset before rebalance. (This is not implemented yet)
   *
   * @param tp     the partition
   * @param offset the offset of the message to check
   * @return true if the message should be skipped. Otherwise false.
   */
  private boolean shouldSkip(TopicPartition tp, long offset) {
    Long hw = _partitionConsumerHighWatermark.get(tp);
    return hw != null && hw > offset;
  }
}
