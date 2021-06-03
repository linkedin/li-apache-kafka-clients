/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.largemessage.ConsumerRecordsProcessResult;
import com.linkedin.kafka.clients.largemessage.ConsumerRecordsProcessor;
import com.linkedin.kafka.clients.largemessage.DeliveredMessageOffsetTracker;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import com.linkedin.kafka.clients.largemessage.MessageAssembler;
import com.linkedin.kafka.clients.largemessage.MessageAssemblerImpl;
import com.linkedin.kafka.clients.largemessage.errors.ConsumerRecordsProcessingException;
import com.linkedin.kafka.clients.utils.CompositeMap;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final String _clientId;
  private final ConsumerRecordsProcessor<K, V> _consumerRecordsProcessor;
  private final LiKafkaConsumerRebalanceListener<K, V> _consumerRebalanceListener;
  private final LiKafkaOffsetCommitCallback _offsetCommitCallback;
  private final boolean _autoCommitEnabled;
  private final long _autoCommitInterval;
  private final boolean _throwExceptionOnInvalidOffsets;
  private final LiOffsetResetStrategy _offsetResetStrategy;
  private long _lastAutoCommitMs;
  private AtomicInteger _offsetInvalidOrOutRangeCount;
  private final Map<MetricName, Metric> _extraMetrics = new HashMap<>(2);

  private ConsumerRecordsProcessResult<K, V> _lastProcessedResult;

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
    _throwExceptionOnInvalidOffsets = configs.getBoolean(LiKafkaConsumerConfig.EXCEPTION_ON_INVALID_OFFSET_RESET_CONFIG);
    _offsetResetStrategy =
        LiOffsetResetStrategy.valueOf(configs.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
    _lastAutoCommitMs = System.currentTimeMillis();
    // We need to set the auto commit to false in KafkaConsumer because it is not large message aware.
    ByteArrayDeserializer byteArrayDeserializer = new ByteArrayDeserializer();
    _kafkaConsumer = new KafkaConsumer<>(configs.configForVanillaConsumer(),
                                         byteArrayDeserializer,
                                         byteArrayDeserializer);
    _clientId = LiKafkaClientsUtils.getClientId(_kafkaConsumer);
    _offsetInvalidOrOutRangeCount = new AtomicInteger(0);

    MetricName skippedRecordsMetricName = new MetricName(
        "records-skipped",
        "lnkd",
        "number of records skipped due to deserialization issues",
        Collections.singletonMap("client-id", _clientId)
    );
    Metric skippedRecordsMetric = new Metric() {
      @Override
      public MetricName metricName() {
        return skippedRecordsMetricName;
      }

      @Override
      public double value() {
        return (double) _consumerRecordsProcessor.getRecordsSkipped();
      }

      @Override
      public Object metricValue() {
        return value();
      }
    };

    MetricName offsetOutOfRangeCounterName = new MetricName(
        "consumer-liclosest-data-loss-estimation",
        "lnkd",
        "counter of how many times the consumer reached OffsetOutOfRangeException or NoOffsetForPartitionException"
            + "with liclosest reset strategy (case 2 and 3b) for potential data loss estimation.",
        Collections.singletonMap("client-id", _clientId)
    );
    Metric offsetOutOfRangeCounter = new Metric() {
      @Override
      public MetricName metricName() {
        return offsetOutOfRangeCounterName;
      }

      @Override
      public double value() {
        return _offsetInvalidOrOutRangeCount.get();
      }

      @Override
      public Object metricValue() {
        return value();
      }
    };

    MetricName consumerOffsetWatermarkSpan = new MetricName(
        "consumer-offset-watermark-span",
        "lnkd",
        "sum of offset-watermark-span for all partitions, where offset-watermark-span of a partition "
            + "shows how far behind the safe offset of a partition is from its watermark.",
        Collections.singletonMap("client-id", _clientId)
    );
    Metric consumerOffsetWatermarkSpanMetric = new Metric() {
      @Override
      public MetricName metricName() {
        return consumerOffsetWatermarkSpan;
      }

      @Override
      public double value() {
        return (double) _consumerRecordsProcessor.getConsumerOffsetWatermarkSpan();
      }

      @Override
      public Object metricValue() {
        return value();
      }
    };

    _extraMetrics.put(skippedRecordsMetricName, skippedRecordsMetric);
    _extraMetrics.put(consumerOffsetWatermarkSpan, consumerOffsetWatermarkSpanMetric);
    _extraMetrics.put(offsetOutOfRangeCounterName, offsetOutOfRangeCounter);

    try {

    // Instantiate segment deserializer if needed.
    Deserializer segmentDeserializer = largeMessageSegmentDeserializer != null ? largeMessageSegmentDeserializer :
        configs.getConfiguredInstance(LiKafkaConsumerConfig.SEGMENT_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    segmentDeserializer.configure(configs.originals(), false);

    // Instantiate message assembler if needed.
    int messageAssemblerCapacity = configs.getInt(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_BUFFER_CAPACITY_CONFIG);
    int messageAssemblerExpirationOffsetGap = configs.getInt(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_CONFIG);
    boolean exceptionOnMessageDropped = configs.getBoolean(LiKafkaConsumerConfig.EXCEPTION_ON_MESSAGE_DROPPED_CONFIG);
    boolean treatBadSegmentsAsPayload = configs.getBoolean(LiKafkaConsumerConfig.TREAT_BAD_SEGMENTS_AS_PAYLOAD_CONFIG);
    MessageAssembler assembler = new MessageAssemblerImpl(messageAssemblerCapacity, messageAssemblerExpirationOffsetGap,
                                                          exceptionOnMessageDropped, segmentDeserializer, treatBadSegmentsAsPayload);

    // Instantiate delivered message offset tracker if needed.
    int maxTrackedMessagesPerPartition = configs.getInt(LiKafkaConsumerConfig.MAX_TRACKED_MESSAGES_PER_PARTITION_CONFIG);
    DeliveredMessageOffsetTracker messageOffsetTracker = new DeliveredMessageOffsetTracker(maxTrackedMessagesPerPartition);

    // Instantiate auditor if needed.
    Auditor<K, V> auditor;
    if (consumerAuditor != null) {
      auditor = consumerAuditor;
      auditor.configure(configs.originals());
    } else {
      auditor = configs.getConfiguredInstance(LiKafkaConsumerConfig.AUDITOR_CLASS_CONFIG, Auditor.class);
    }
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
        messageOffsetTracker, auditor, _kafkaConsumer::committed);

    // Instantiate consumer rebalance listener
    _consumerRebalanceListener = new LiKafkaConsumerRebalanceListener<>(_consumerRecordsProcessor,
                                                                        this, _autoCommitEnabled);

    // Instantiate offset commit callback.
    _offsetCommitCallback = new LiKafkaOffsetCommitCallback();
    _lastProcessedResult = null;
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
  public void subscribe(Pattern pattern) {
    _kafkaConsumer.subscribe(pattern);
  }

  @Override
  public void unsubscribe() {
    // Clear all the state of the topic in consumer record processor.
    _consumerRecordsProcessor.clear();
    _kafkaConsumer.unsubscribe();
  }

  private ConsumerRecords<K, V> poll(long timeout, boolean includeMetadataInTimeout) {
    ConsumerRecords<K, V> processedRecords;
    // We will keep polling until timeout.
    long now = System.currentTimeMillis();
    long deadline = now + timeout;
    do {
      ConsumerRecordsProcessingException crpe;

      // throw exception to user if the current active (un-paused) topic-partitions has exceptions
      Set<TopicPartition> unPausedTopicPartitions = new HashSet<>(_kafkaConsumer.assignment());
      unPausedTopicPartitions.removeAll(_kafkaConsumer.paused());
      crpe = handleRecordProcessingException(unPausedTopicPartitions);
      if (crpe != null) {
        throw crpe;
      }

      if (_autoCommitEnabled && now > _lastAutoCommitMs + _autoCommitInterval) {
        commitAsync();
        _lastAutoCommitMs = now;
      }
      ConsumerRecords<byte[], byte[]> rawRecords = ConsumerRecords.empty();
      try {
        if (includeMetadataInTimeout) {
          rawRecords = _kafkaConsumer.poll(Duration.ofMillis(deadline - now));
        } else {
          rawRecords = _kafkaConsumer.poll(deadline - now);
        }
      } catch (OffsetOutOfRangeException | NoOffsetForPartitionException oe) {
        handleInvalidOffsetException(oe);
        // force throw exception if exception.on.invalid.offset.reset is set to true
        if (_throwExceptionOnInvalidOffsets) {
          throw oe;
        }
      }

      _lastProcessedResult = _consumerRecordsProcessor.process(rawRecords);
      processedRecords = _lastProcessedResult.consumerRecords();
      // Clear the internal reference.
      _lastProcessedResult.clearRecords();
      // Rewind offset if there are processing exceptions.
      seekToCurrentOffsetsOnRecordProcessingExceptions();

      // this is an optimization
      // if no records were processed try to throw exception in current poll()
      if (processedRecords.isEmpty()) {
        crpe = handleRecordProcessingException(null);
        if (crpe != null) {
          throw crpe;
        }
      }

      now = System.currentTimeMillis();
    } while (processedRecords.isEmpty() && now < deadline);
    return processedRecords;
  }

  /**
   * We still need this API for at least one specific case in Kafka-Rest. poll((long) 0) is used by Kafka-Rest's background threads
   * to do rebalance. We cannot use poll(Duration 0) because poll(Duration) includes metadata update time in the Duration,
   * so we end up exiting too soon to finish rebalance. poll((long) 0) on the other hand will wait for as long as it takes
   * to rebalance, which is the desired behavior.
   * @param timeout timeout in milliseconds for poll. Excludes metadata update time
   * @return {@link ConsumerRecords}
   */
  @Override
  @Deprecated
  public ConsumerRecords<K, V> poll(long timeout) {
    return poll(timeout, false);
  }

  @Override
  public ConsumerRecords<K, V> poll(Duration timeout) {
    return poll(timeout.toMillis(), true);
  }

  @Override
  public void commitSync() {
    // Preserve the high watermark.
    commitOffsets(currentOffsetAndMetadataMap(), false, null, true, null);
  }

  @Override
  public void commitSync(Duration timeout) {
    commitOffsets(currentOffsetAndMetadataMap(), false, null, true, timeout);
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // ignore the high watermark.
    commitOffsets(offsets, true, null, true, null);
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    commitOffsets(offsets, true, null, true, timeout);
  }

  @Override
  public void commitAsync() {
    commitOffsets(currentOffsetAndMetadataMap(), false, null, false, null);
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    // preserve the high watermark.
    commitOffsets(currentOffsetAndMetadataMap(), false, callback, false, null);
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    // Ignore the high watermark.
    commitOffsets(offsets, true, callback, false, null);
  }

  // Private function to avoid duplicate code.
  // timeout is used when sync == true only.
  private void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets,
                             boolean ignoreConsumerHighWatermark,
                             OffsetCommitCallback callback,
                             boolean sync,
                             Duration timeout) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
        _consumerRecordsProcessor.safeOffsetsToCommit(offsets, ignoreConsumerHighWatermark);
    if (sync) {
      if (timeout == null) {
        LOG.trace("Committing offsets synchronously: {}", offsetsToCommit);
        _kafkaConsumer.commitSync(offsetsToCommit);
      } else {
        LOG.trace("Committing offsets synchronously with timeout {} ms: {}", timeout.toMillis(), offsetsToCommit);
        _kafkaConsumer.commitSync(offsetsToCommit, timeout);
      }
    } else {
      LOG.trace("Committing offsets asynchronously: {}", offsetsToCommit);
      _offsetCommitCallback.setUserCallback(callback);
      _kafkaConsumer.commitAsync(offsetsToCommit, _offsetCommitCallback);
    }
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    // current offsets are being moved so don't throw cached exceptions in poll.
    clearRecordProcessingException();

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
    // current offsets are being moved so don't throw cached exceptions in poll.
    clearRecordProcessingException();

    _kafkaConsumer.seekToBeginning(partitions);
    for (TopicPartition tp : partitions) {
      _consumerRecordsProcessor.clear(tp);
      // We set the high watermark to 0 if user is seeking to beginning.
      _consumerRecordsProcessor.setPartitionConsumerHighWaterMark(tp, 0L);
    }
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    // current offsets are being moved so don't throw cached exceptions in poll.
    clearRecordProcessingException();

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
    // current offsets are being moved so don't throw cached exceptions in poll.
    clearRecordProcessingException();

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
    return positionMain(partition, null);
  }

  @Override
  public long position(TopicPartition partition, Duration timeout) {
    return positionMain(partition, timeout);
  }

  // A help method for position() to avoid code duplication
  private long positionMain(TopicPartition partition, Duration timeout) {
    // Not handling large message here. The position will be actual position.
    while (true) { // In kafka 0.10.x we can get an unbounded number of invalid offset exception
      try {
        if (timeout == null) {
          return _kafkaConsumer.position(partition);
        } else {
          return _kafkaConsumer.position(partition, timeout);
        }
      } catch (OffsetOutOfRangeException | NoOffsetForPartitionException oe) {
        handleInvalidOffsetException(oe);
      }
    }
  }

  private ConsumerRecordsProcessingException handleRecordProcessingException(Collection<TopicPartition> topicPartitions) {
    if (_lastProcessedResult == null || !_lastProcessedResult.hasException()) {
      return null;
    }

    ConsumerRecordsProcessingException crpe = null;
    if (topicPartitions == null || topicPartitions.isEmpty()) {
      // seek past offset for all topic-partitions that hit an exception
      _lastProcessedResult.offsets().forEach((tp, o) -> _kafkaConsumer.seek(tp, o.getResumeOffset()));
      crpe = _lastProcessedResult.exception();
    } else {
      // seek past offset for topic-partition in the collection that hit an exception
      if (_lastProcessedResult.hasError(topicPartitions)) {
        Map<TopicPartition, ConsumerRecordsProcessResult<K, V>.OffsetPair> offsets = _lastProcessedResult.offsets();
        topicPartitions.forEach(tp -> {
          if (offsets.containsKey(tp)) {
            _kafkaConsumer.seek(tp, offsets.get(tp).getResumeOffset());
          }
        });
        crpe = _lastProcessedResult.exception(topicPartitions);
      }

      // if topic-partitions don't have an exception then just drop cached exceptions and move-on
    }
    _lastProcessedResult = null;
    return crpe;
  }

  private void seekToCurrentOffsetsOnRecordProcessingExceptions() {
    // seek to offset which had an exception
    if (_lastProcessedResult != null && _lastProcessedResult.hasException()) {
      _lastProcessedResult.offsets().forEach((k, v) -> _kafkaConsumer.seek(k, v.getCurrentOffset()));
    }
  }

  private void clearRecordProcessingException() {
    if (_lastProcessedResult != null && _lastProcessedResult.hasException()) {
      LOG.warn("Clearing all Record Processing Exceptions", _lastProcessedResult.exception());
      _lastProcessedResult = null;
    }
  }

  /**
   * We don't let the underlying open source consumer reset offsets so we need to do that here.
   */
  private void handleInvalidOffsetException(InvalidOffsetException oe) throws InvalidOffsetException {
    switch (_offsetResetStrategy) {
      case EARLIEST:
        LOG.warn("Invalid positions of {} due to {}. Resetting position to the earliest.", oe.partitions(),
            oe.getClass().getSimpleName());
        seekToBeginning(oe.partitions());
        break;
      case LATEST:
        LOG.warn("Invalid positions of {} due to {}. Resetting position to the latest.", oe.partitions(), oe.getClass().getSimpleName());
        seekToEnd(oe.partitions());
        break;
      case LICLOSEST:
        LOG.warn("Invalid positions of {} due to {}. Resetting position to the li_closest.", oe.partitions(),
            oe.getClass().getSimpleName());
        handleLiClosestResetStrategy(oe);
        break;
      default:
        throw oe;
    }
  }

  /**
   * This method handles the OffsetResetStrategy="LICLOSEST" offset reset strategy.
   *
   * The semantics of this strategy is defined as follows:
   * Consumer will {@link #seekToBeginning(Collection)} when InvalidOffsetException occurs due to:
   * 1. New Consumer / Expired Commit Offset
   * 2. Fall-off Start (fetch offset < LSO)
   *
   * Consumer will {@link #seekToEnd(Collection)} when InvalidOffsetException occurs due to:
   * 3a. Fall-off End (fetch offset > LEO): Consumer will seek to the end
   * 3b. Fall-off End (fetch offset <= LEO): Consumer will seek to the fetched offset
   *
   * Note: Offset to which we reset may not necessarily be a safe offset. This method invokes 2 blocking calls and does
   * ignore large-message tracking metadata. If we are unable to calculate the bounds, it will throw an
   * IllegalStateException.
   *
   * Design details can be found here - https://docs.google.com/document/d/1zKGXxZiyiRkLJ_d0FCoGALfAo0N7k3hh9NFYhJrbPsw/edit#
   * @param oe InvalidOffsetException
   */
  private void handleLiClosestResetStrategy(InvalidOffsetException oe) {
    if (oe instanceof NoOffsetForPartitionException) {  // Case 1
      LOG.info("No valid offsets found. Rewinding to the earliest");
      seekToBeginning(oe.partitions());
    } else if (oe instanceof OffsetOutOfRangeException) {
      Map<TopicPartition, Long> seekBeginningPartitions = new HashMap<>();
      Map<TopicPartition, Long> seekEndPartitions = new HashMap<>();
      Map<TopicPartition, Long> seekFetchedOffsetPartitions = new HashMap<>();
      Set<TopicPartition> boundsUnknownPartitions = new HashSet<>();

      Map<TopicPartition, Long> beginningOffsets = beginningOffsets(oe.partitions());
      Map<TopicPartition, Long> endOffsets = endOffsets(oe.partitions());

      ((OffsetOutOfRangeException) oe).offsetOutOfRangePartitions().forEach((tp, fetchedOffset) -> {
        long beginningOffset = beginningOffsets.getOrDefault(tp, -1L);
        long endOffset = endOffsets.getOrDefault(tp, -1L);
        if (beginningOffset != -1L && endOffset != -1L) {
          if (beginningOffset > fetchedOffset) {  // Case 2
            seekBeginningPartitions.put(tp, beginningOffset);
            _offsetInvalidOrOutRangeCount.getAndIncrement();
            return;
          }
          if (endOffset < fetchedOffset) {  // Case 3a
            LOG.debug("Closest offset computed for topic partition {} is the log end offset {}. ", tp, fetchedOffset);
            seekEndPartitions.put(tp, endOffset);
          } else {  // Case 3b: endOffset >= fetchedOffset
            LOG.debug("Closest offset computed for topic partition {} is the fetched offset {}. ", tp, fetchedOffset);
            seekFetchedOffsetPartitions.put(tp, fetchedOffset);
            _offsetInvalidOrOutRangeCount.getAndIncrement();
          }
        } else {
          // can't handle reset if the either bound values are not known
          // ideally, this should never happen since the listoffsets protocol always returns all requested offset or none
          boundsUnknownPartitions.add(tp);
        }
      });

      if (!boundsUnknownPartitions.isEmpty()) {
        throw new IllegalStateException("Couldn't figure out the closest offset for these topic partitions " +
            boundsUnknownPartitions + "Aborting..");
      }

      if (!seekBeginningPartitions.isEmpty()) {
        LOG.info("Offsets are out of range for partitions {}. Seeking to the beginning offsets returned", seekBeginningPartitions);
        seekBeginningPartitions.forEach(this::seekAndClear);
      }
      if (!seekEndPartitions.isEmpty()) {
        LOG.info("Offsets are out of range for partitions {}. Seeking to the end offsets returned", seekEndPartitions);
        seekEndPartitions.forEach(this::seekAndClear);
      }
      if (!seekFetchedOffsetPartitions.isEmpty()) {
        LOG.info("Seeking to fetched offsets for topic partitions {}. This may indicate a potential loss of data.",
            seekFetchedOffsetPartitions.keySet());
        seekFetchedOffsetPartitions.forEach(this::seekAndClear);
      }
    } else {
      throw oe;
    }
  }

  private void seekAndClear(TopicPartition tp, Long offset) {
    _kafkaConsumer.seek(tp, offset);
    _consumerRecordsProcessor.clear(tp);
    _consumerRecordsProcessor.setPartitionConsumerHighWaterMark(tp, offset >= 1 ? offset - 1 : 0);
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    return committedMain(partition, null);
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
    return committedMain(partition, timeout);
  }

  // A help method for committed() to avoid code duplication.
  private OffsetAndMetadata committedMain(TopicPartition partition, Duration timeout) {
    // Not handling large message here. The committed will be the actual committed value.
    // The returned metadata includes the user committed offset and the user committed metadata, separated by the
    // first comma.
    OffsetAndMetadata offsetAndMetadata;
    if (timeout == null) {
      offsetAndMetadata = _kafkaConsumer.committed(partition);
    } else {
      offsetAndMetadata = _kafkaConsumer.committed(partition, timeout);
    }
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
    //noinspection unchecked
    return new CompositeMap<>((Map<MetricName, Metric>) _kafkaConsumer.metrics(), _extraMetrics);
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return _kafkaConsumer.partitionsFor(topic);
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
    return _kafkaConsumer.partitionsFor(topic, timeout);
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return _kafkaConsumer.listTopics();
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
    return _kafkaConsumer.listTopics(timeout);
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
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
    return _kafkaConsumer.offsetsForTimes(timestampsToSearch, timeout);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    return _kafkaConsumer.beginningOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    return _kafkaConsumer.beginningOffsets(partitions, timeout);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    return _kafkaConsumer.endOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    return _kafkaConsumer.endOffsets(partitions, timeout);
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
    LOG.info("Shutting down ...");
    long start = System.currentTimeMillis();
    if (_autoCommitEnabled) {
      commitSync();
    }
    _kafkaConsumer.close();
    _consumerRecordsProcessor.close();
    LOG.info("Shutdown complete in {} millis", (System.currentTimeMillis() - start));
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
    close(Duration.ofMillis(timeUnit.toMillis(timeout)));
  }

  @Override
  public void close(Duration timeout) {
    LOG.info("Shutting down in {}...", timeout);
    long start = System.currentTimeMillis();
    long budget = timeout.toMillis();
    long deadline = start + budget;
    if (_autoCommitEnabled) {
      commitSync(); //TODO - find a way around this
    }
    long remaining = System.currentTimeMillis() - deadline; //could be negative
    _kafkaConsumer.close(Duration.ofMillis(Math.max(0, remaining)));
    remaining = System.currentTimeMillis() - deadline; //could be negative
    _consumerRecordsProcessor.close(Math.max(0, remaining), TimeUnit.MILLISECONDS);
    LOG.info("Shutdown complete in {} millis", (System.currentTimeMillis() - start));
  }

  @Override
  public void wakeup() {
    _kafkaConsumer.wakeup();
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
    Set<TopicPartition> knownPartitions = _consumerRecordsProcessor.knownPartitions();
    for (TopicPartition tp : _kafkaConsumer.assignment()) {
      if (delivered.containsKey(tp)) {
        // Case 1
        offsetAndMetadataMap.put(tp, new OffsetAndMetadata(delivered.get(tp) + 1, ""));
      } else {
        // No message has been delivered from the partition.
        Long earliestTrackedOffset = _consumerRecordsProcessor.earliestTrackedOffset(tp);
        if (earliestTrackedOffset != null) {
          // Case 2, some message was consumed. Use the earliest tracked offset to avoid losing messages.
          offsetAndMetadataMap.put(tp, new OffsetAndMetadata(position(tp), ""));
        } else if (knownPartitions.contains(tp)) {
            // Case 4 user called seek() or we have not consumed anything on this partition,
            // use the current position to commit.  This gets corrected later when computing the safe offsets if
            // a consumer hwm exists in Kaka broker already.
            offsetAndMetadataMap.put(tp, new OffsetAndMetadata(position(tp), ""));
        } else {
          //Case 4.  Never consumed from partiton.
        }
      }
    }
    LOG.trace("Current offset and metadata map: {}", offsetAndMetadataMap);
    return offsetAndMetadataMap;
  }
}
