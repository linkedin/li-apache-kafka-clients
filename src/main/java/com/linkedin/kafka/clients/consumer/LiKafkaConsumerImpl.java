/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.largemessage.ConsumerRecordsProcessor;
import com.linkedin.kafka.clients.largemessage.DeliveredMessageOffsetTracker;
import com.linkedin.kafka.clients.largemessage.MessageAssembler;
import com.linkedin.kafka.clients.largemessage.MessageAssemblerImpl;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.utils.HeaderParser;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import java.nio.ByteBuffer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The implementation of {@link LiKafkaConsumer}
 * LiKafkaConsumerImpl wraps an underlying {@link KafkaConsumer}. On top of that, LiKafkaConsumerImpl provides the
 * additional functions of handling large messages
 * (@see <a href=http://www.slideshare.net/JiangjieQin/handle-large-messages-in-apache-kafka-58692297>design details</a>)
 * and auditing.
 * <p>
 * Creating a LiKafkaConsumerImpl is very similar to creating a {@link KafkaConsumer}. Besides the configurations
 * required by {@link KafkaConsumer}, LiKafkaConsumerImpl takes the following additional configurations for handling
 * large messages:
 * <ul>
 * <li>message.assembler.buffer.capacity</li>
 * <li>message.assembler.expiration.offset.gap</li>
 * <li>max.tracked.messages.per.partition</li>
 * <li>exception.on.message.dropped</li>
 * </ul>
 * and it also takes a "auditor.class" configuration for auditing. (see {@link LiKafkaConsumerConfig} for more
 * configuration details).
 */
public class LiKafkaConsumerImpl<K, V> implements LiKafkaConsumer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaConsumerImpl.class);
  private final Consumer<byte[], byte[]> _kafkaConsumer;
  private final ConsumerRecordsProcessor _consumerRecordsProcessor;
  private final LiKafkaConsumerRebalanceListener<K, V> _consumerRebalanceListener;
  private final LiKafkaOffsetCommitCallback _offsetCommitCallback;
  private final boolean _autoCommitEnabled;
  private final long _autoCommitInterval;
  private long _lastAutoCommitMs;
  private final Deserializer<K> _keyDeserializer;
  private final Deserializer<V> _valueDeserializer;
  private final Auditor<K, V> _auditor;
  private final HeaderParser _headerParser;

  public LiKafkaConsumerImpl(Properties props) {
    this(new LiKafkaConsumerConfig(props), null, null, null);
  }

  public LiKafkaConsumerImpl(Map<String, Object> configs) {
    this(new LiKafkaConsumerConfig(configs), null, null, null);
  }

  public LiKafkaConsumerImpl(Map<String, Object> configs,
                             Deserializer<K> keyDeserializer,
                             Deserializer<V> valueDeserializer,
                             Auditor<K, V> consumerAuditor) {
    this(new LiKafkaConsumerConfig(configs), keyDeserializer, valueDeserializer, consumerAuditor);
  }

  @SuppressWarnings("unchecked")
  private LiKafkaConsumerImpl(LiKafkaConsumerConfig configs,
                             Deserializer<K> keyDeserializer,
                             Deserializer<V> valueDeserializer,
                             Auditor<K, V> consumerAuditor) {

    _autoCommitEnabled = configs.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    _autoCommitInterval = configs.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
    _lastAutoCommitMs = System.currentTimeMillis();
    // We need to set the auto commit to false in KafkaConsumer because it is not large message aware.
    ByteArrayDeserializer byteArrayDeserializer = new ByteArrayDeserializer();
    _kafkaConsumer = new KafkaConsumer<>(configs.configWithAutoCommitDisabled(),
                                         byteArrayDeserializer,
                                         byteArrayDeserializer);

    // Instantiate message assembler if needed.
    int messageAssemblerCapacity = configs.getInt(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_BUFFER_CAPACITY_CONFIG);
    int messageAssemblerExpirationOffsetGap = configs.getInt(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_CONFIG);
    boolean exceptionOnMessageDropped = configs.getBoolean(LiKafkaConsumerConfig.EXCEPTION_ON_MESSAGE_DROPPED_CONFIG);
    MessageAssembler assembler = new MessageAssemblerImpl(messageAssemblerCapacity, messageAssemblerExpirationOffsetGap,
        exceptionOnMessageDropped);

    // Instantiate delivered message offset tracker if needed.
    int maxTrackedMessagesPerPartition = configs.getInt(LiKafkaConsumerConfig.MAX_TRACKED_MESSAGES_PER_PARTITION_CONFIG);
    DeliveredMessageOffsetTracker messageOffsetTracker = new DeliveredMessageOffsetTracker(maxTrackedMessagesPerPartition);

    // Instantiate auditor if needed.
    _auditor = consumerAuditor != null ? consumerAuditor :
        configs.getConfiguredInstance(LiKafkaConsumerConfig.AUDITOR_CLASS_CONFIG, Auditor.class);
    _auditor.configure(configs.originals());
    _auditor.start();

    // Instantiate key and value deserializer if needed.
    _keyDeserializer = keyDeserializer != null ? keyDeserializer :
        configs.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    _keyDeserializer.configure(configs.originals(), true);
    _valueDeserializer = valueDeserializer != null ? valueDeserializer :
        configs.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    _valueDeserializer.configure(configs.originals(), false);

    // Instantiate consumer record processor
    _consumerRecordsProcessor = new ConsumerRecordsProcessor(assembler, messageOffsetTracker);

    // Instantiate consumer rebalance listener
    _consumerRebalanceListener = new LiKafkaConsumerRebalanceListener<>(_consumerRecordsProcessor,
        this, _autoCommitEnabled);

    // Instantiate offset commit callback.
    _offsetCommitCallback = new LiKafkaOffsetCommitCallback();

    _headerParser = new HeaderParser(configs.getString(LiKafkaConsumerConfig.LI_KAFKA_MAGIC_CONFIG));
  }

  @Override
  public Set<TopicPartition> assignment() {
    return _kafkaConsumer.assignment();
  }

  @Override
  public Set<String> subscription() {
    return _kafkaConsumer.subscription();
  }

  @Override
  public void subscribe(Collection<String> topics) {
    subscribe(topics, new NoOpConsumerRebalanceListener());
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    Set<String> newSubscription = new HashSet<>(topics);
    // TODO: This is a hot fix for KAFKA-3664 and should be removed after the issue is fixed.
    commitSync();
    for (TopicPartition tp : _kafkaConsumer.assignment()) {
      if (!newSubscription.contains(tp.topic())) {
        _consumerRecordsProcessor.clear(tp);
      }
    }
    _consumerRebalanceListener.setUserListener(callback);
    _kafkaConsumer.subscribe(new ArrayList<>(topics), _consumerRebalanceListener);
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    Set<TopicPartition> partitionSet = new HashSet<>(partitions);
    for (TopicPartition tp : assignment()) {
      if (!partitionSet.contains(tp)) {
        _consumerRecordsProcessor.clear(tp);
      }
    }
    _kafkaConsumer.assign(partitions);
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    if (callback != null) {
      _consumerRebalanceListener.setUserListener(callback);
    }
    _kafkaConsumer.subscribe(pattern, _consumerRebalanceListener);
  }

  @Override
  public void unsubscribe() {
    // Clear all the state of the topic in consumer record processor.
    _consumerRecordsProcessor.clear();
    _kafkaConsumer.unsubscribe();
  }

  @Override
  public ConsumerRecords<K, V> poll(long timeout) {
    long startMs = System.currentTimeMillis();
    Collection<ExtensibleConsumerRecord<byte[], byte[]>> xRecords;
    // We will keep polling until timeout.
    long now = startMs;
    long expireMs = startMs + timeout;
    do {
      if (_autoCommitEnabled && now > _lastAutoCommitMs + _autoCommitInterval) {
        commitAsync();
        _lastAutoCommitMs = now;
      }
      ConsumerRecords<byte[], byte[]> rawRecords = _kafkaConsumer.poll(expireMs - now);

      // Check if we have enough consumer high watermark for a partition. The consumer high watermark is cleared during
      // rebalance. We make this check so that after rebalance we do not deliver duplicate messages to the user.
      if (!rawRecords.isEmpty() && _consumerRecordsProcessor.numConsumerHighWaterMarks() < assignment().size()) {
        for (TopicPartition tp : rawRecords.partitions()) {
          if (_consumerRecordsProcessor.consumerHighWaterMarkForPartition(tp) == null) {
            OffsetAndMetadata offsetAndMetadata = committed(tp);
            if (offsetAndMetadata != null) {
              long hw = offsetAndMetadata.offset();
              _consumerRecordsProcessor.setPartitionConsumerHighWaterMark(tp, hw);
            }
          }
        }
      }

      xRecords = toXRecords(rawRecords);
      xRecords = _consumerRecordsProcessor.process(xRecords);

      now = System.currentTimeMillis();
    } while (xRecords.isEmpty() && now < startMs + timeout);

    Map<TopicPartition, List<ConsumerRecord<K, V>>> consumerRecordsMap = new HashMap<>();
    for (ExtensibleConsumerRecord<byte[], byte[]> xRecord : xRecords) {
      ExtensibleConsumerRecord<K, V> userRecord = deserialize(xRecord);
      if (_auditor != null) {
        long totalBytes = userRecord.headersSize() + userRecord.serializedKeySize() + userRecord.serializedValueSize();
        _auditor.record(userRecord.topic(), userRecord.key(), userRecord.value(), userRecord.timestamp(), 1L, totalBytes,
            AuditType.SUCCESS);
      }

      TopicPartition topicPartition = new TopicPartition(userRecord.topic(), userRecord.partition());
      List<ConsumerRecord<K, V>> listForTopicPartition = consumerRecordsMap.get(topicPartition);
      if (listForTopicPartition == null) {
        listForTopicPartition = new ArrayList<>();
        consumerRecordsMap.put(topicPartition, listForTopicPartition);
      }

      listForTopicPartition.add(userRecord);
    }

    ConsumerRecords<K, V> consumerRecords = new ConsumerRecords<>(consumerRecordsMap);
    return consumerRecords;
  }

  private ExtensibleConsumerRecord<K, V> deserialize(ExtensibleConsumerRecord<byte[], byte[]> record) {
    K key = _keyDeserializer.deserialize(record.topic(), record.key());
    V value = _valueDeserializer.deserialize(record.topic(), record.value());
    ExtensibleConsumerRecord<K, V> deserializedRecord =
        new ExtensibleConsumerRecord<>(record.topic(), record.partition(), record.offset(),
            record.timestamp(), record.timestampType(),
            record.checksum(),
            record.serializedKeySize(), record.serializedValueSize(),
            key, value,
            record.headers(), record.headersSize());
    return deserializedRecord;
  }

  private List<ExtensibleConsumerRecord<byte[], byte[]>> toXRecords(ConsumerRecords<byte[], byte[]> rawRecords) {
    List<ExtensibleConsumerRecord<byte[], byte[]>> xRecords = new ArrayList<>(rawRecords.count());
    for (ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
      xRecords.add(toXRecord(rawRecord));
    }
    return xRecords;
  }

  private ExtensibleConsumerRecord<byte[], byte[]> toXRecord(ConsumerRecord<byte[], byte[]> rawRecord) {
    ByteBuffer rawByteBuffer = ByteBuffer.wrap(rawRecord.value() == null ? new byte[0] : rawRecord.value());
    if (!_headerParser.isHeaderMessage(rawByteBuffer)) {
      return new ExtensibleConsumerRecord<>(rawRecord.topic(), rawRecord.partition(), rawRecord.offset(), rawRecord.timestamp(), rawRecord.timestampType(),
          rawRecord.checksum(), rawRecord.serializedKeySize(), rawRecord.serializedValueSize(), rawRecord.key(), rawRecord.value(), null, 0);
    }

    int headerSize = rawByteBuffer.getInt();
    rawByteBuffer.limit(rawByteBuffer.position() + headerSize);
    ByteBuffer headerByteBuffer = rawByteBuffer.slice();
    LazyHeaderListMap headers = new LazyHeaderListMap(headerByteBuffer);
    rawByteBuffer.position(headerSize + _headerParser.magicSize() + 4);
    rawByteBuffer.limit(rawByteBuffer.capacity());
    int valueSize = rawByteBuffer.getInt();
    byte[] value = new byte[valueSize];
    rawByteBuffer.get(value);

    if (rawByteBuffer.hasRemaining()) {
      throw new IllegalStateException("Failed to consume all bytes in message buffer.");
    }
    //TODO: recompute checksum?
    return new ExtensibleConsumerRecord<>(rawRecord.topic(), rawRecord.partition(), rawRecord.offset(), rawRecord.timestamp(),
        rawRecord.timestampType(), rawRecord.checksum(), rawRecord.serializedKeySize(), valueSize, rawRecord.key(),
         value, headers, headerSize + _headerParser.magicSize() /* magic size*/);
  }

  @Override
  public void commitSync() {
    commitSync(toOffsetAndMetadataMap(_consumerRecordsProcessor.delivered()));
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = getOffsetsToCommit(offsets);
    LOG.trace("Committing offsets synchronously: {}", offsetsToCommit);
    _kafkaConsumer.commitSync(offsetsToCommit);
  }

  @Override
  public void commitAsync() {
    commitAsync(toOffsetAndMetadataMap(_consumerRecordsProcessor.delivered()), null);
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    commitAsync(toOffsetAndMetadataMap(_consumerRecordsProcessor.delivered()), callback);
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = getOffsetsToCommit(offsets);
    LOG.trace("Committing offsets asynchronously: {}", offsetsToCommit);
    _offsetCommitCallback.setUserCallback(callback);
    _kafkaConsumer.commitAsync(offsetsToCommit, _offsetCommitCallback);
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    // We only check the consumer record processor if user is seeking backward.
    Long lastDeliveredFromPartition = _consumerRecordsProcessor.delivered(partition);
    // Do nothing if user wants to seek to the last delivered + 1.
    if (lastDeliveredFromPartition != null && offset == lastDeliveredFromPartition + 1) {
      return;
    }
    // Now we really need to seek. We only do the sanity check if the user is seeking backward. If user is seeking
    // forward, there is no large message awareness.
    long offsetToSeek = offset;
    if (lastDeliveredFromPartition != null && offset <= lastDeliveredFromPartition) {
      // We need to seek to the smaller one of the starting offset and safe offset to ensure we do not lose
      // any message starting from that offset.
      Long mostRecentlyConsumed = _consumerRecordsProcessor.closestDeliveredUpTo(partition, offset);
      if (mostRecentlyConsumed == offset) {
        // User is seeking to a valid consumed offset.
        offsetToSeek = Math.min(_consumerRecordsProcessor.startingOffset(partition, offset),
            _consumerRecordsProcessor.safeOffset(partition, offset));
      } else if (mostRecentlyConsumed < offset) {
        // User is seeking to an offset that is not corresponding to a consumed message.
        // We use the safe offset of the most recently delivered offset in this case.
        offsetToSeek = _consumerRecordsProcessor.safeOffset(partition, mostRecentlyConsumed);
      }
    }
    _kafkaConsumer.seek(partition, offsetToSeek);
    _consumerRecordsProcessor.clear(partition);
    // We set the consumer high watermark of this partition to the offset to seek so the messages with smaller offset
    // won't be delivered to user.
    _consumerRecordsProcessor.setPartitionConsumerHighWaterMark(partition, offset);
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    _kafkaConsumer.seekToBeginning(partitions);
    for (TopicPartition tp : partitions) {
      _consumerRecordsProcessor.clear(tp);
    }
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    _kafkaConsumer.seekToEnd(partitions);
    for (TopicPartition tp : partitions) {
      _consumerRecordsProcessor.clear(tp);
    }
  }

  @Override
  public void seekToCommitted(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      OffsetAndMetadata offsetAndMetadata = _kafkaConsumer.committed(tp);
      if (offsetAndMetadata == null) {
        throw new NoOffsetForPartitionException(tp);
      }
      _kafkaConsumer.seek(tp, offsetAndMetadata.offset());
      _consumerRecordsProcessor.clear(tp);
      long lw = LiKafkaClientsUtils.offsetFromWrappedMetadata(offsetAndMetadata.metadata());
      _consumerRecordsProcessor.setPartitionConsumerHighWaterMark(tp, lw);
    }
  }

  @Override
  public long position(TopicPartition partition) {
    // Not handling large message here. The position will be actual position.
    return _kafkaConsumer.position(partition);
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    // Not handling large message here. The committed will be the actual committed value.
    // The returned metadata includes the user committed offset and the user committed metadata, separated by the
    // first comma.
    OffsetAndMetadata offsetAndMetadata = _kafkaConsumer.committed(partition);
    if (offsetAndMetadata != null) {
      String rawMetadata = offsetAndMetadata.metadata();
      long userOffset = LiKafkaClientsUtils.offsetFromWrappedMetadata(rawMetadata);
      String userMetadata = LiKafkaClientsUtils.metadataFromWrappedMetadata(rawMetadata);
      offsetAndMetadata = new OffsetAndMetadata(userOffset, userMetadata);
    }
    return offsetAndMetadata;
  }

  @Override
  public long committedSafeOffset(TopicPartition tp) {
    OffsetAndMetadata rawOffsetAndMetadata = _kafkaConsumer.committed(tp);
    if (rawOffsetAndMetadata == null) {
      return -1L;
    }
    return rawOffsetAndMetadata.offset();
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return _kafkaConsumer.metrics();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return _kafkaConsumer.partitionsFor(topic);
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return _kafkaConsumer.listTopics();
  }

  @Override
  public Set<TopicPartition> paused() {
    return _kafkaConsumer.paused();
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    _kafkaConsumer.pause(partitions);
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    _kafkaConsumer.resume(partitions);
  }

  @Override
  public long safeOffset(TopicPartition tp, long messageOffset) {
    return _consumerRecordsProcessor.safeOffset(tp, messageOffset);
  }

  @Override
  public long safeOffset(TopicPartition tp) {
    return _consumerRecordsProcessor.safeOffset(tp);
  }

  @Override
  public Map<TopicPartition, Long> safeOffsets() {
    Map<TopicPartition, Long> safeOffsets = new HashMap<>();
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : _consumerRecordsProcessor.safeOffsets().entrySet()) {
      safeOffsets.put(entry.getKey(), entry.getValue().offset());
    }
    return safeOffsets;
  }

  @Override
  public void close() {
    if (_autoCommitEnabled) {
      commitSync();
    }
    _kafkaConsumer.close();
    _consumerRecordsProcessor.close();
    _auditor.close();
    _keyDeserializer.close();
    _valueDeserializer.close();
  }

  @Override
  public void wakeup() {
    _kafkaConsumer.wakeup();
  }

  /**
   * Helper function to get the large message aware TopicPartition to OffsetAndMetadata mapping.
   *
   * @param offsets The user provided TopicPartition to OffsetsAndMetadata mapping.
   * @return The translated large message aware TopicPartition to OffsetAndMetadata mapping.
   */
  private Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = _consumerRecordsProcessor.safeOffsets(offsets);
    // If user did not consume any message before the first commit, in this case user will pass in the last
    // committed message offsets. We simply use the last committed safe offset.
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
      OffsetAndMetadata committed = _kafkaConsumer.committed(entry.getKey());
      if (committed != null) {
        Long committedUserOffset = LiKafkaClientsUtils.offsetFromWrappedMetadata(committed.metadata());
        if (entry.getValue().offset() == committedUserOffset) {
          long safeOffset = committed.offset();
          String userMetadata = entry.getValue().metadata();
          String wrappedMetadata = LiKafkaClientsUtils.wrapMetadataWithOffset(userMetadata, committedUserOffset);
          offsetsToCommit.put(entry.getKey(), new OffsetAndMetadata(safeOffset, wrappedMetadata));
        }
      }
    }
    return offsetsToCommit;
  }

  /**
   * A helper function that converts the last delivered offset map to the offset to commit.
   *
   * @param lastDelivered the last delivered offset map
   * @return the offsetAndMetadata map ready to commit.
   */
  private Map<TopicPartition, OffsetAndMetadata> toOffsetAndMetadataMap(Map<TopicPartition, Long> lastDelivered) {
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : lastDelivered.entrySet()) {
      offsetAndMetadataMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue() + 1, ""));
    }
    return offsetAndMetadataMap;
  }

}
