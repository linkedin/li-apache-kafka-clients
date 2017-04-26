/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.largemessage.ConsumerRecordsProcessor;
import com.linkedin.kafka.clients.largemessage.DeliveredMessageOffsetTracker;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import com.linkedin.kafka.clients.largemessage.MessageAssembler;
import com.linkedin.kafka.clients.largemessage.MessageAssemblerImpl;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
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
 * <li>segment.deserializer.class</li>
 * </ul>
 * and it also takes a "auditor.class" configuration for auditing. (see {@link LiKafkaConsumerConfig} for more
 * configuration details).
 */
public class LiKafkaConsumerImpl<K, V> implements LiKafkaConsumer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaConsumerImpl.class);
  private final Consumer<byte[], byte[]> _kafkaConsumer;
  private final ConsumerRecordsProcessor<K, V> _consumerRecordsProcessor;
  private final LiKafkaConsumerRebalanceListener<K, V> _consumerRebalanceListener;
  private final LiKafkaOffsetCommitCallback _offsetCommitCallback;
  private final boolean _autoCommitEnabled;
  private final long _autoCommitInterval;
  private final OffsetResetStrategy _offsetResetStrategy;
  private long _lastAutoCommitMs;

  public LiKafkaConsumerImpl(Properties props) {
    this(new LiKafkaConsumerConfig(props), null, null, null, null);
  }

  public LiKafkaConsumerImpl(Properties props,
                             Deserializer<K> keyDeserializer,
                             Deserializer<V> valueDeserializer,
                             Deserializer<LargeMessageSegment> largeMessageSegmentDeserializer,
                             Auditor<K, V> consumerAuditor) {
    this(new LiKafkaConsumerConfig(props), keyDeserializer, valueDeserializer, largeMessageSegmentDeserializer, consumerAuditor);
  }

  public LiKafkaConsumerImpl(Map<String, Object> configs) {
    this(new LiKafkaConsumerConfig(configs), null, null, null, null);
  }

  public LiKafkaConsumerImpl(Map<String, Object> configs,
                             Deserializer<K> keyDeserializer,
                             Deserializer<V> valueDeserializer,
                             Deserializer<LargeMessageSegment> largeMessageSegmentDeserializer,
                             Auditor<K, V> consumerAuditor) {
    this(new LiKafkaConsumerConfig(configs), keyDeserializer, valueDeserializer, largeMessageSegmentDeserializer, consumerAuditor);
  }

  @SuppressWarnings("unchecked")
  private LiKafkaConsumerImpl(LiKafkaConsumerConfig configs,
                              Deserializer<K> keyDeserializer,
                              Deserializer<V> valueDeserializer,
                              Deserializer<LargeMessageSegment> largeMessageSegmentDeserializer,
                              Auditor<K, V> consumerAuditor) {

    _autoCommitEnabled = configs.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    _autoCommitInterval = configs.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
    _offsetResetStrategy =
        OffsetResetStrategy.valueOf(configs.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
    _lastAutoCommitMs = System.currentTimeMillis();
    // We need to set the auto commit to false in KafkaConsumer because it is not large message aware.
    ByteArrayDeserializer byteArrayDeserializer = new ByteArrayDeserializer();
    _kafkaConsumer = new KafkaConsumer<>(configs.configForVanillaConsumer(),
                                         byteArrayDeserializer,
                                         byteArrayDeserializer);
try {

    // Instantiate segment deserializer if needed.
    Deserializer segmentDeserializer = largeMessageSegmentDeserializer != null ? largeMessageSegmentDeserializer :
        configs.getConfiguredInstance(LiKafkaConsumerConfig.SEGMENT_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    segmentDeserializer.configure(configs.originals(), false);

    // Instantiate message assembler if needed.
    int messageAssemblerCapacity = configs.getInt(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_BUFFER_CAPACITY_CONFIG);
    int messageAssemblerExpirationOffsetGap = configs.getInt(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_CONFIG);
    boolean exceptionOnMessageDropped = configs.getBoolean(LiKafkaConsumerConfig.EXCEPTION_ON_MESSAGE_DROPPED_CONFIG);
    MessageAssembler assembler = new MessageAssemblerImpl(messageAssemblerCapacity, messageAssemblerExpirationOffsetGap,
                                                          exceptionOnMessageDropped, segmentDeserializer);

    // Instantiate delivered message offset tracker if needed.
    int maxTrackedMessagesPerPartition = configs.getInt(LiKafkaConsumerConfig.MAX_TRACKED_MESSAGES_PER_PARTITION_CONFIG);
    DeliveredMessageOffsetTracker messageOffsetTracker = new DeliveredMessageOffsetTracker(maxTrackedMessagesPerPartition);

    // Instantiate auditor if needed.
    Auditor<K, V> auditor = consumerAuditor != null ? consumerAuditor :
        configs.getConfiguredInstance(LiKafkaConsumerConfig.AUDITOR_CLASS_CONFIG, Auditor.class);
    auditor.configure(configs.originals());
    auditor.start();

    // Instantiate key and value deserializer if needed.
    Deserializer<K> kDeserializer = keyDeserializer != null ? keyDeserializer :
        configs.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    kDeserializer.configure(configs.originals(), true);
    Deserializer<V> vDeserializer = valueDeserializer != null ? valueDeserializer :
        configs.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    vDeserializer.configure(configs.originals(), false);

    // Instantiate consumer record processor
    _consumerRecordsProcessor = new ConsumerRecordsProcessor<>(assembler, kDeserializer, vDeserializer,
                                                               messageOffsetTracker, auditor);

    // Instantiate consumer rebalance listener
    _consumerRebalanceListener = new LiKafkaConsumerRebalanceListener<>(_consumerRecordsProcessor,
                                                                        this, _autoCommitEnabled);

    // Instantiate offset commit callback.
    _offsetCommitCallback = new LiKafkaOffsetCommitCallback();
    } catch (Exception e) {
      _kafkaConsumer.close();
      throw e;
    }
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
    ConsumerRecords<K, V> processedRecords;
    // We will keep polling until timeout.
    long now = startMs;
    long expireMs = startMs + timeout;
    do {
      if (_autoCommitEnabled && now > _lastAutoCommitMs + _autoCommitInterval) {
        commitAsync();
        _lastAutoCommitMs = now;
      }
      ConsumerRecords<byte[], byte[]> rawRecords = ConsumerRecords.empty();
      try {
         rawRecords = _kafkaConsumer.poll(expireMs - now);
      } catch (OffsetOutOfRangeException | NoOffsetForPartitionException oe) {
        switch (_offsetResetStrategy) {
          case EARLIEST:
            oe.partitions().forEach(_consumerRecordsProcessor::clear);
            _kafkaConsumer.seekToBeginning(oe.partitions());
            break;
          case LATEST:
            oe.partitions().forEach(_consumerRecordsProcessor::clear);
            _kafkaConsumer.seekToEnd(oe.partitions());
            break;
          default:
            throw oe;
        }
      }
      // Check if we have enough high watermark for a partition. The high watermark is cleared during rebalance.
      // We make this check so that after rebalance we do not deliver duplicate messages to the user.
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
      processedRecords = _consumerRecordsProcessor.process(rawRecords);
      now = System.currentTimeMillis();
    } while (processedRecords.isEmpty() && now < startMs + timeout);
    return processedRecords;
  }

  @Override
  public void commitSync() {
    // Preserve the high watermark.
    commitOffsets(currentOffsetAndMetadataMap(), false, null, true);
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // ignore the high watermark.
    commitOffsets(offsets, true, null, true);
  }

  @Override
  public void commitAsync() {
    commitOffsets(currentOffsetAndMetadataMap(), false, null, false);
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    // preserve the high watermark.
    commitOffsets(currentOffsetAndMetadataMap(), false, callback, false);
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    // Ignore the high watermark.
    commitOffsets(offsets, true, callback, false);
  }

  // Private function to avoid duplicate code.
  private void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets,
                             boolean ignoreConsumerHighWatermark,
                             OffsetCommitCallback callback,
                             boolean sync) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = getOffsetsToCommit(offsets, ignoreConsumerHighWatermark);
    if (sync) {
      LOG.trace("Committing offsets synchronously: {}", offsetsToCommit);
      _kafkaConsumer.commitSync(offsetsToCommit);
    } else {
      LOG.trace("Committing offsets asynchronously: {}", offsetsToCommit);
      _offsetCommitCallback.setUserCallback(callback);
      _kafkaConsumer.commitAsync(offsetsToCommit, _offsetCommitCallback);
    }
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    // The offset seeks is a complicated case, there are four situations to be handled differently.
    // 1. Before the earliest consumed message. An OffsetNotTrackedException will be thrown in this case.
    // 2. At or after the earliest consumed message but before the first delivered message. We will seek to the earliest
    //    tracked offset in this case to avoid losing messages.
    // 3. After the first delivered message but before the last delivered message, we seek to the safe offset of the
    //    closest delivered message before the sought to offset.
    // 4. After the lastDelivered message. We seek to the user provided offset.
    //
    // In addition, there are two special cases we can handle more intelligently.
    // 5. User seeks to the last committed offsets. We will reload the committed information instead of naively seeking
    //    in this case.
    // 6. User seeks to the current position. Do nothing, i.e. not clean up the internal information.
    Long lastDeliveredFromPartition = _consumerRecordsProcessor.delivered(partition);
    // Do nothing if user wants to seek to the last delivered + 1.
    if (lastDeliveredFromPartition != null && offset == lastDeliveredFromPartition + 1) {
      // Case 6
      return;
    }
    OffsetAndMetadata committed = committed(partition);
    if (committed != null && committed.offset() == offset) {
      // Case 5: If the user is seeking to the last committed offset, we use the last committed information.
      seekToCommitted(Collections.singleton(partition));
    } else {
      // Now we really need to seek. We only do the sanity check if the user is seeking backward. If user is seeking
      // forward, there is no large message awareness.
      Long offsetToSeek = offset;
      // Case 4 if the following if statement is false
      if (lastDeliveredFromPartition != null && offset <= lastDeliveredFromPartition) {
        // We need to seek to the smaller one of the starting offset and safe offset to ensure we do not lose
        // any message starting from that offset.
        Long closestDeliveredBeforeOffset = _consumerRecordsProcessor.closestDeliveredUpTo(partition, offset);
        if (closestDeliveredBeforeOffset != null && closestDeliveredBeforeOffset == offset) {
          // Case 2
          offsetToSeek = Math.min(_consumerRecordsProcessor.startingOffset(partition, offset),
                                  _consumerRecordsProcessor.safeOffset(partition, offset));
        } else if (closestDeliveredBeforeOffset != null && closestDeliveredBeforeOffset < offset) {
          // case 3
          offsetToSeek = _consumerRecordsProcessor.safeOffset(partition, closestDeliveredBeforeOffset);
        } else {
          // Case 1
          // If there is no recently delivered offset, we use the earliest tracked offset.
          offsetToSeek = _consumerRecordsProcessor.earliestTrackedOffset(partition);
          assert offsetToSeek != null;
        }
      }
      _kafkaConsumer.seek(partition, offsetToSeek);
      _consumerRecordsProcessor.clear(partition);
      // We set the low watermark of this partition to the offset to seek so the messages with smaller offset
      // won't be delivered to user.
      _consumerRecordsProcessor.setPartitionConsumerHighWaterMark(partition, offset);
    }
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    _kafkaConsumer.seekToBeginning(partitions);
    for (TopicPartition tp : partitions) {
      _consumerRecordsProcessor.clear(tp);
      // We set the high watermark to 0 if user is seeking to beginning.
      _consumerRecordsProcessor.setPartitionConsumerHighWaterMark(tp, 0L);
    }
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    _kafkaConsumer.seekToEnd(partitions);
    for (TopicPartition tp : partitions) {
      _consumerRecordsProcessor.clear(tp);
      // We set the high watermark to 0 if user is seeking to end. This is needed to prevent the consumer from
      // retrieving high watermark from the committed offsets.
      _consumerRecordsProcessor.setPartitionConsumerHighWaterMark(tp, 0L);
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
      Long hw = LiKafkaClientsUtils.offsetFromWrappedMetadata(offsetAndMetadata.metadata());
      if (hw == null) {
        hw = offsetAndMetadata.offset();
      }
      _consumerRecordsProcessor.setPartitionConsumerHighWaterMark(tp, hw);
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
      Long userOffset = LiKafkaClientsUtils.offsetFromWrappedMetadata(rawMetadata);
      String userMetadata;
      if (userOffset == null) {
        userOffset = offsetAndMetadata.offset();
        userMetadata = offsetAndMetadata.metadata();
      } else {
        userMetadata = LiKafkaClientsUtils.metadataFromWrappedMetadata(rawMetadata);
      }
      offsetAndMetadata = new OffsetAndMetadata(userOffset, userMetadata);
    }
    return offsetAndMetadata;
  }

  @Override
  public Long committedSafeOffset(TopicPartition tp) {
    OffsetAndMetadata rawOffsetAndMetadata = _kafkaConsumer.committed(tp);
    if (rawOffsetAndMetadata == null || rawOffsetAndMetadata.metadata().isEmpty()) {
      return null;
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
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
    return _kafkaConsumer.offsetsForTimes(timestampsToSearch);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    return _kafkaConsumer.beginningOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    return _kafkaConsumer.endOffsets(partitions);
  }

  @Override
  public Long safeOffset(TopicPartition tp, long messageOffset) {
    return _consumerRecordsProcessor.safeOffset(tp, messageOffset);
  }

  @Override
  public Long safeOffset(TopicPartition tp) {
    return _consumerRecordsProcessor.safeOffset(tp);
  }

  @Override
  public Map<TopicPartition, Long> safeOffsets() {
    Map<TopicPartition, Long> safeOffsets = new HashMap<>();
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : _consumerRecordsProcessor.safeOffsetsToCommit().entrySet()) {
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
  private Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                    boolean ignoreHighWaterMark) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
        _consumerRecordsProcessor.safeOffsetsToCommit(offsets, ignoreHighWaterMark);
    // If user did not consume any message before the first commit, in this case user will pass in the last
    // committed message offsets. We simply use the last committed safe offset.
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
      OffsetAndMetadata committed = _kafkaConsumer.committed(entry.getKey());
      if (committed != null) {
        Long committedUserOffset = LiKafkaClientsUtils.offsetFromWrappedMetadata(committed.metadata());
        if (committedUserOffset != null && entry.getValue().offset() == committedUserOffset) {
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
   * This function is tricky and handles the following scenarios:
   * 1. Some messages has been delivered (returned to the user) from a partition.
   * 2. No message was delivered, but some messages were consumed (read from broker).
   * 3. No message was delivered, and no message was consumed.
   * 4. User called seek().
   *
   * Generally speaking, this method is only responsible for taking care of the offset to commit, but not caring
   * about the high watermark. The high watermark will be taken care of by
   * {@link ConsumerRecordsProcessor#safeOffsetsToCommit(Map, boolean)}, it will ensure the high watermark never rewind unless
   * user explicitly did so by calling seek() or provided a specific offset.
   *
   * @return the offsetAndMetadata map ready to commit.
   */
  private Map<TopicPartition, OffsetAndMetadata> currentOffsetAndMetadataMap() {
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
    Map<TopicPartition, Long> delivered = _consumerRecordsProcessor.delivered();
    for (TopicPartition tp : _kafkaConsumer.assignment()) {
      if (delivered.containsKey(tp)) {
        // Case 1
        offsetAndMetadataMap.put(tp, new OffsetAndMetadata(delivered.get(tp) + 1, ""));
      } else {
        // No message has been delivered from the partition.
        Long earliestTrackedOffset = _consumerRecordsProcessor.earliestTrackedOffset(tp);
        Long hw = _consumerRecordsProcessor.consumerHighWaterMarkForPartition(tp);
        if (earliestTrackedOffset != null) {
          // We may need to update the high watermark in case there are two rebalances happened back to back, in that
          // case we may lose high watermark if we don't fetch it from the server.
          if (hw == null) {
            OffsetAndMetadata committed = committed(tp);
            if (committed != null) {
              _consumerRecordsProcessor.setPartitionConsumerHighWaterMark(tp, committed.offset());
            }
          }
          // Case 2, some message was consumed. Use the earliest tracked offset to avoid losing messages.
          offsetAndMetadataMap.put(tp, new OffsetAndMetadata(_kafkaConsumer.position(tp), ""));
        } else {
          // No message was consumed.
          if (hw == null) {
            // Case 3, no message is consumed, no message is delivered. Do nothing to avoid overriding the committed
            // high watermark on the server. We can also get the committed high watermark from the server, but it
            // is unnecessary work.
          } else {
            // Case 4, user called seek(), we use the current position to commit.
            offsetAndMetadataMap.put(tp, new OffsetAndMetadata(_kafkaConsumer.position(tp), ""));
          }
        }
      }
    }
    LOG.trace("Current offset and metadata map: {}", offsetAndMetadataMap);
    return offsetAndMetadataMap;
  }
}
