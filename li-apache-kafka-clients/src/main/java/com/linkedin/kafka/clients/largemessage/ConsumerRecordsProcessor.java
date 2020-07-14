/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.common.LargeMessageHeaderValue;
import com.linkedin.kafka.clients.largemessage.errors.SkippableException;
import com.linkedin.kafka.clients.security.TopicEncrypterDecrypterManager;
import com.linkedin.kafka.clients.utils.Constants;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.PrimitiveEncoderDecoder;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.ExtendedDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.clients.largemessage.MessageAssembler.AssembleResult.INCOMPLETE_RESULT;


/**
 * This class processes consumer records returned by {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(long)}
 */
public class ConsumerRecordsProcessor<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerRecordsProcessor.class);

  /**
   * Holds the current and stored (initial consumer high watermark).
   */
  private static final class ConsumerHighWatermarkState {
    /** The actual offset committed (i.e. the safe offset) not the consumer high watermark. */
    private final long _initialCommittedOffset;

    /** The consumer, high, watermark fetched from the Kafka server. */
    private final long _initialConsumerHighWatermark;

    /** This gets updated to be the last message to be delivered to the caller. */
    private long _currentConsumerHighWatermark;

    ConsumerHighWatermarkState(long initialConsumerHighWatermark, long initialCommittedOffset) {
      _initialConsumerHighWatermark = initialConsumerHighWatermark;
      _currentConsumerHighWatermark = initialConsumerHighWatermark;
      _initialCommittedOffset = initialCommittedOffset;
    }

    ConsumerHighWatermarkState() {
      this(-1, -1);
    }

    boolean isCurrentDefined() {
      return _currentConsumerHighWatermark != -1;
    }

    boolean isInitialDefined() {
      return _initialConsumerHighWatermark != -1;
    }

    boolean isCommittedOffsetDefined() {
      return _initialCommittedOffset != -1;
    }
  }

  private final MessageAssembler _messageAssembler;
  private final Deserializer<K> _keyDeserializer;
  private final Deserializer<V> _valueDeserializer;
  private final DeliveredMessageOffsetTracker _deliveredMessageOffsetTracker;
  private final Map<TopicPartition, ConsumerHighWatermarkState> _partitionConsumerHighWatermark;
  private final Auditor<K, V> _auditor;
  private final Function<TopicPartition, ConsumerHighWatermarkState> _storedConsumerHighWatermark;
  private final DeserializeStrategy<V> _deserializeStrategy;
  private final TopicEncrypterDecrypterManager _topicEncrypterDecrypterManager;
  private long recordsSkipped = 0;

  /**
   *
   * @param messageAssembler non-null.  Assembles large segments segments
   * @param keyDeserializer non-null.
   * @param valueDeserializer non-null
   * @param deliveredMessageOffsetTracker non-null.  Keeps a history of safe offsets.
   * @param auditor This may be null otherwise auditing is called when messages are complete.
   * @param storedOffset non-null.  A function that returns the offset information stored in Kafka.   This may
   *                      be a blocking call and should return null if the information is not available.
   * @param topicEncrypterDecrypterManager This may be null otherwise decryption will be executed when messages are complete
   */
  public ConsumerRecordsProcessor(MessageAssembler messageAssembler,
                                  Deserializer<K> keyDeserializer,
                                  Deserializer<V> valueDeserializer,
                                  DeliveredMessageOffsetTracker deliveredMessageOffsetTracker,
                                  Auditor<K, V> auditor,
                                  Function<TopicPartition, OffsetAndMetadata> storedOffset,
                                  TopicEncrypterDecrypterManager topicEncrypterDecrypterManager) {
    _messageAssembler = messageAssembler;
    _keyDeserializer = keyDeserializer;
    _valueDeserializer = valueDeserializer;
    _deliveredMessageOffsetTracker = deliveredMessageOffsetTracker;
    _auditor = auditor;
    _topicEncrypterDecrypterManager = topicEncrypterDecrypterManager;
    _partitionConsumerHighWatermark = new HashMap<>();
    if (_auditor == null) {
      LOG.info("Auditing is disabled because no auditor is defined.");
    }
    _storedConsumerHighWatermark = (topicPartition) -> {
      OffsetAndMetadata offsetAndMetadata = storedOffset.apply(topicPartition);

      Long consumerHighWatermark = null;
      if (offsetAndMetadata != null) {
        consumerHighWatermark = LiKafkaClientsUtils.offsetFromWrappedMetadata(offsetAndMetadata.metadata());
      }

      if (consumerHighWatermark == null) {
        return new ConsumerHighWatermarkState();
      }
      return new ConsumerHighWatermarkState(consumerHighWatermark, offsetAndMetadata.offset());
    };
    // Performance optimization to avoid a call to instanceof for every deserialization call
    if (_valueDeserializer instanceof ExtendedDeserializer) {
      _deserializeStrategy = new DeserializeStrategy<V>() {
        @Override
        public V deserialize(String topic, Headers headers, byte[] data) {
          return ((ExtendedDeserializer<V>) _valueDeserializer).deserialize(topic, headers, data);
        }
      };
    } else {
      _deserializeStrategy = new DeserializeStrategy<V>() {
        @Override
        public V deserialize(String topic, Headers headers, byte[] data) {
          return _valueDeserializer.deserialize(topic, data);
        }
      };
    }
  }

  public ConsumerRecordsProcessor(MessageAssembler messageAssembler,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer,
      DeliveredMessageOffsetTracker deliveredMessageOffsetTracker,
      Auditor<K, V> auditor,
      Function<TopicPartition, OffsetAndMetadata> storedOffset) {
    this(messageAssembler, keyDeserializer, valueDeserializer, deliveredMessageOffsetTracker, auditor, storedOffset,
        null);
  }

  /**
   * This method filters out the incomplete message segment records.
   *
   * @param consumerRecords The consumer records to be filtered.
   * @return filtered consumer records.
   */
  public ConsumerRecordsProcessResult<K, V> process(ConsumerRecords<byte[], byte[]> consumerRecords) {
    ConsumerRecordsProcessResult<K, V> result = new ConsumerRecordsProcessResult<>();
    for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
      TopicPartition tp = new TopicPartition(record.topic(), record.partition());
      if (result.hasError(tp)) {
        continue;
      }
      long offset = record.offset();
      try {
        ConsumerRecord<K, V> handledRecord = handleConsumerRecord(record);
        result.addRecord(tp, handledRecord);
      } catch (SkippableException e) {
        recordsSkipped++;
        LOG.warn("Exception thrown when processing message with offset {} from partition {}. record skipped.", offset, tp, e);
      } catch (RuntimeException e) {
        LOG.warn("Exception thrown when processing message with offset {} from partition {}", offset, tp, e);
        result.recordException(tp, offset, e);
      }
    }
    return result;
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
   * @param ignoreConsumerHighWatermark whether to ignore the current consumer high watermark.
   * @return the safe offset map that user should use to commit offsets.
   */
  public Map<TopicPartition, OffsetAndMetadata> safeOffsetsToCommit(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
                                                                    boolean ignoreConsumerHighWatermark) {
    Map<TopicPartition, OffsetAndMetadata> safeOffsetsToCommit = new HashMap<>(offsetsToCommit.size());
    for (TopicPartition tp : offsetsToCommit.keySet()) {
      ConsumerHighWatermarkState highWatermarkState =
          _partitionConsumerHighWatermark.computeIfAbsent(tp, _storedConsumerHighWatermark);
      long origOffset = offsetsToCommit.get(tp).offset();
      Long safeOffsetToCommit = origOffset;
      Long earliestTrackedOffset = _deliveredMessageOffsetTracker.earliestTrackedOffset(tp);
      // When origOffset is smaller or equals to the earliest tracked offset, we simply use it without additional
      // lookup. Notice that in this case, (origOffset - 1) will be out of the tracked range.
      if (earliestTrackedOffset != null && origOffset > earliestTrackedOffset) {
        // We need to find the previous delivered offset from this partition and use its safe offset.
        Long previousDeliveredOffset = _deliveredMessageOffsetTracker.closestDeliveredUpTo(tp, origOffset - 1);
        Long latestDelivered = _deliveredMessageOffsetTracker.delivered(tp); //latest ever delivered for this partition

        if (Objects.equals(previousDeliveredOffset, latestDelivered)) {
          //user is attempting to commit "current location" (as opposed to arbitrary seek shenanigans)
          //in this case we must also take into account the earliest offset for which we still have
          //any hope of delivering a large message (maybe we dropped a lot of them due to low memory)
          long offsetToCheck = latestDelivered == null ? origOffset : latestDelivered;
          safeOffsetToCommit = _messageAssembler.safeOffset(tp, offsetToCheck);
        } else {
          if (previousDeliveredOffset != null) {
            safeOffsetToCommit = _deliveredMessageOffsetTracker.safeOffset(tp, previousDeliveredOffset);
          } else {
            safeOffsetToCommit = _messageAssembler.safeOffset(tp, latestDelivered);
          }
        }
      }

      // We need to combine the metadata with the high watermark. High watermark should , generally not rewind.
      long highWatermarkToCommit;
      if (ignoreConsumerHighWatermark) {
        highWatermarkToCommit = origOffset;
      } else if (safeOffsetToCommit == origOffset && highWatermarkState.isInitialDefined() &&
          highWatermarkState.isCommittedOffsetDefined() && safeOffsetToCommit == highWatermarkState._initialCommittedOffset) {
        //We didn't consume anything and so need to preserve the old high watermark in the commit.
        highWatermarkToCommit = Math.max(highWatermarkState._currentConsumerHighWatermark, highWatermarkState._initialConsumerHighWatermark);
      } else if (highWatermarkState.isCurrentDefined()) {
        highWatermarkToCommit = Math.max(origOffset, highWatermarkState._currentConsumerHighWatermark);
      } else {
        // No initial or current state so this is a new commit.
        highWatermarkToCommit = origOffset;
      }

      safeOffsetToCommit = Math.min(safeOffsetToCommit, origOffset);
      String wrappedMetadata = LiKafkaClientsUtils.wrapMetadataWithOffset(offsetsToCommit.get(tp).metadata(), highWatermarkToCommit);
      OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(safeOffsetToCommit, wrappedMetadata);
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
    _partitionConsumerHighWatermark.computeIfAbsent(tp, _storedConsumerHighWatermark)._currentConsumerHighWatermark = offset;
  }

  /**
   * Partitions known to the consumer records processor since they have consumer, high  watermarks.  This may be
   * a subset of the assigned partitions.
   * @return non-null
   * which is why this is interesting to call otherwise just call consumer.assigned()
   */
  public Set<TopicPartition> knownPartitions() {
    return Collections.unmodifiableSet(_partitionConsumerHighWatermark.keySet());
  }

  /**
   * Clear all the high watermarks tracked by the consumer record processor.
   */
  public void clearAllConsumerHighWaterMarks() {
    _partitionConsumerHighWatermark.clear();
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

  public void close(long timeout, TimeUnit timeUnit) {
    _messageAssembler.close();
    _auditor.close(timeout, timeUnit);
  }

  public long getRecordsSkipped() {
    return recordsSkipped;
  }

  private ConsumerRecord<K, V> handleConsumerRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
    TopicPartition tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
    ConsumerRecord<K, V> handledRecord = null;
    K key = _keyDeserializer.deserialize(tp.topic(), consumerRecord.key());
    // Create a new copy of the headers
    Headers headers = new RecordHeaders(consumerRecord.headers());
    Header largeMessageHeader = headers.lastHeader(Constants.LARGE_MESSAGE_HEADER);
    Header encryptionHeader = headers.lastHeader(Constants.ENCRYPTION_HEADER);

    byte[] valueBytes = parseAndMaybeTrackRecord(tp, consumerRecord.offset(), consumerRecord.value(), largeMessageHeader);
    if (valueBytes != INCOMPLETE_RESULT) {
      if (encryptionHeader != null && valueBytes != null) {
        valueBytes = _topicEncrypterDecrypterManager.getEncrypterDecrypter(consumerRecord.topic()).decrypt(valueBytes);

      }
      V value = (V) _deserializeStrategy.deserialize(tp.topic(), consumerRecord.headers(), valueBytes);
      if (_auditor != null) {
        long sizeInBytes = (consumerRecord.key() == null ? 0 : consumerRecord.key().length) +
            (valueBytes == null ? 0 : valueBytes.length);
        _auditor.record(_auditor.auditToken(key, value), tp.topic(), consumerRecord.timestamp(), 1L,
                        sizeInBytes, AuditType.SUCCESS);
      }

      _partitionConsumerHighWatermark.computeIfAbsent(tp, _storedConsumerHighWatermark)._currentConsumerHighWatermark = consumerRecord.offset();
      if (largeMessageHeader != null) {
        LargeMessageHeaderValue largeMessageHeaderValue = LargeMessageHeaderValue.fromBytes(largeMessageHeader.value());
        // Once the large message header value is parsed, remove any such key from record headers
        headers.remove(Constants.LARGE_MESSAGE_HEADER);
        largeMessageHeaderValue = new LargeMessageHeaderValue(
            largeMessageHeaderValue.getType(),
            LargeMessageHeaderValue.EMPTY_UUID,
            LargeMessageHeaderValue.INVALID_SEGMENT_ID,
            largeMessageHeaderValue.getNumberOfSegments(),
            largeMessageHeaderValue.getType() == LargeMessageHeaderValue.LEGACY ?
                LargeMessageHeaderValue.INVALID_MESSAGE_SIZE : largeMessageHeaderValue.getMessageSizeInBytes()
        );
        headers.add(Constants.LARGE_MESSAGE_HEADER, LargeMessageHeaderValue.toBytes(largeMessageHeaderValue));
      }

      // Introduce a safe offset header if and only if the safe offset is not the same as the high watermark
      // Note: Safe offset is the last consumed record offset + 1
      Long safeOffset = safeOffset(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()));
      if (safeOffset != null && safeOffset != consumerRecord.offset() + 1) {
        headers.add(Constants.SAFE_OFFSET_HEADER, PrimitiveEncoderDecoder.encodeLong(safeOffset));
      }

      handledRecord = new ConsumerRecord<>(
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          consumerRecord.timestamp(),
          consumerRecord.timestampType(),
          consumerRecord.checksum(),
          consumerRecord.serializedKeySize(),
          valueBytes == null ? 0 : valueBytes.length,
          _keyDeserializer.deserialize(consumerRecord.topic(), consumerRecord.key()),
          value,
          headers
      );
    }
    return handledRecord;
  }

  private byte[] parseAndMaybeTrackRecord(TopicPartition tp,
                                          long messageOffset,
                                          byte[] bytes,
                                          Header header) {
    MessageAssembler.AssembleResult assembledResult;
    if (header == null) {
      assembledResult = _messageAssembler.assemble(tp, messageOffset, bytes);
    } else {
      assembledResult = _messageAssembler.assemble(tp, messageOffset, bytes, header);
    }

    if (assembledResult.messageBytes() != INCOMPLETE_RESULT) {
      LOG.trace("Got message {} from partition {}", messageOffset, tp);
      boolean shouldSkip = shouldSkip(tp, messageOffset);
      // The safe offset is the smaller one of the current message offset + 1 and current safe offset.
      long safeOffset = Math.min(messageOffset + 1, _messageAssembler.safeOffset(tp, messageOffset));
      _deliveredMessageOffsetTracker.track(tp,
                                           messageOffset,
                                           safeOffset,
                                           assembledResult.messageStartingOffset(),
                                           !shouldSkip);
      // We skip the messages whose offset is smaller than the high watermark.
      if (shouldSkip) {
        LOG.trace("Skipping message {} from partition {} because its offset is smaller than the high watermark",
                  messageOffset, tp);
        return INCOMPLETE_RESULT;
      } else {
        return assembledResult.messageBytes();
      }
    } else {
      _deliveredMessageOffsetTracker.addNonMessageOffset(tp, messageOffset);
      return INCOMPLETE_RESULT;
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
    ConsumerHighWatermarkState consumerHighWatermarkState =
        _partitionConsumerHighWatermark.computeIfAbsent(tp, _storedConsumerHighWatermark);
    long hw = 0;
    if (consumerHighWatermarkState.isCurrentDefined()) {
      hw = consumerHighWatermarkState._currentConsumerHighWatermark;
    } else if (consumerHighWatermarkState.isInitialDefined()) {
      hw = consumerHighWatermarkState._initialConsumerHighWatermark;
    } else {
      return false;
    }
    return hw > offset;
  }

  private static interface DeserializeStrategy<V> {
    V deserialize(String topic, Headers headers, byte[] data);
  }
}
