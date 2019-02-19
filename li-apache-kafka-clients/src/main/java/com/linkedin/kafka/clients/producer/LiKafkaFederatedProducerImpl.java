/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.largemessage.LargeMessageCallback;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import com.linkedin.kafka.clients.largemessage.MessageSplitter;
import com.linkedin.kafka.clients.largemessage.MessageSplitterImpl;
import com.linkedin.kafka.clients.largemessage.errors.SkippableException;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClient;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a producer implementation that works with a federated Kafka cluster, which consists of one or more physical
 * Kafka clusters.
 */
public class LiKafkaFederatedProducerImpl<K, V> implements LiKafkaProducer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaFederatedProducerImpl.class);

  // Per-cluster producer context
  private class PerClusterProducerContext<K, V> {
    private final ClusterDescriptor _cluster;
    private Producer<byte[], byte[]> _producer;
    /*package private for testing*/ Auditor<K, V> _auditor;

    // A counter of the threads in the middle of sending messages. This is needed to ensure when we close the producer
    // everything is audited.
    private final LongAdder _numThreadsInSend = new LongAdder();
    private volatile boolean _closed;

    // This is null if the underlying producer does not have an implementation for time-bounded flush
    private final Method _boundedFlushMethod;
    private final LongAdder _boundedFlushThreadCount = new LongAdder();

    public PerClusterProducerContext(ClusterDescriptor cluster, LiKafkaProducerConfig configs,
        Producer<byte[], byte[]> producer) {
      _cluster = cluster;
      _producer = producer;

      // TODO: This hack remains until bounded flush is added to apache/kafka
      Method producerSupportsBoundedFlush;
      try {
        producerSupportsBoundedFlush = _producer.getClass().getMethod("flush", long.class, TimeUnit.class);
      } catch (NoSuchMethodException e) {
        LOG.warn("Wrapped KafkaProducer does not support time-bounded flush.", e);
        producerSupportsBoundedFlush = null;
      }
      _boundedFlushMethod = producerSupportsBoundedFlush;

      // TODO: add an option to pass custom auditor
      _auditor = configs.getConfiguredInstance(LiKafkaProducerConfig.AUDITOR_CLASS_CONFIG, Auditor.class, _producer);
      _auditor.start();

      _closed = false;
    }

    public ClusterDescriptor cluster() {
      return _cluster;
    }

    public Producer<byte[], byte[]> producer() {
      return _producer;
    }

    public Auditor<K, V> auditor() {
      return _auditor;
    }

    public LongAdder numThreadsInSend() {
      return _numThreadsInSend;
    }

    public boolean closed() {
      return _closed;
    }

    public void setClosed(boolean value) {
      _closed = value;
    }
  }

  // The cluster group this client is talking to
  private ClusterGroupDescriptor _clusterGroup;

  // The client for the metadata service which serves cluster and topic metadata
  private MetadataServiceClient _mdsClient;

  // The id of this client assigned by the metadata service
  private UUID _clientId;

  // Per cluster producers, which sends raw bytes
  private Map<ClusterDescriptor, PerClusterProducerContext<K, V>> _producers;

  // Raw byte producer builder for creating per-cluster producer
  private RawProducerBuilder _producerBuilder;

  // Producer configs common to all clusters
  private LiKafkaProducerConfig _commonProducerConfigs;

  // Large message settings
  private final boolean _largeMessageEnabled;
  private final int _maxMessageSegmentSize;
  private final MessageSplitter _messageSplitter;
  private final boolean _largeMessageSegmentWrappingRequired;

  // Serializers
  private Serializer<K> _keySerializer;
  private Serializer<V> _valueSerializer;

  private final UUIDFactory<K, V> _uuidFactory;

  public LiKafkaFederatedProducerImpl(Properties props) {
    this(new LiKafkaProducerConfig(props), null, null, null, null, null);
  }

  public LiKafkaFederatedProducerImpl(Properties props,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Serializer<LargeMessageSegment> largeMessageSegmentSerializer,
      MetadataServiceClient mdsClient,
      RawProducerBuilder producerBuilder) {
    this(new LiKafkaProducerConfig(props), keySerializer, valueSerializer, largeMessageSegmentSerializer, mdsClient,
        producerBuilder);
  }

  public LiKafkaFederatedProducerImpl(Map<String, ?> configs) {
    this(new LiKafkaProducerConfig(configs), null, null, null, null, null);
  }

  public LiKafkaFederatedProducerImpl(Map<String, ?> configs,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Serializer<LargeMessageSegment> largeMessageSegmentSerializer,
      MetadataServiceClient mdsClient,
      RawProducerBuilder producerBuilder) {
    this(new LiKafkaProducerConfig(configs), keySerializer, valueSerializer, largeMessageSegmentSerializer, mdsClient,
        producerBuilder);
  }

  @SuppressWarnings("unchecked")
  private LiKafkaFederatedProducerImpl(LiKafkaProducerConfig configs,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Serializer<LargeMessageSegment> largeMessageSegmentSerializer,
      MetadataServiceClient mdsClient,
      RawProducerBuilder producerBuilder) {
    _commonProducerConfigs = configs;
    _clusterGroup = new ClusterGroupDescriptor(configs.getString(LiKafkaProducerConfig.CLUSTER_ENVIRONMENT_CONFIG),
        configs.getString(LiKafkaProducerConfig.CLUSTER_GROUP_CONFIG));

    // Each per-cluster producer and auditor will be intantiated by the passed-in producer builder when the client
    // begins to produce to that cluster. If a null builder is passed, create a default one, which builds KafkaProducer.
    _producers = new HashMap<ClusterDescriptor, PerClusterProducerContext<K, V>>();
    _producerBuilder = producerBuilder != null ? producerBuilder : new RawProducerBuilder();

    try {
      // Instantiate metadata service client if necessary.
        _mdsClient = mdsClient != null ? mdsClient :
            configs.getConfiguredInstance(LiKafkaProducerConfig.METADATA_SERVICE_CLIENT_CLASS_CONFIG,
                MetadataServiceClient.class);

      // Register this federated client with the metadata service. The metadata service will assign a UUID to this
      // client, which will be used for later interaction between the metadata service and the client.
      //
      // Registration may also return further information such as the metadata server version and any protocol settings.
      // We assume that such information will be kept and used by the metadata service client itself.
      _clientId = mdsClient.registerFederatedClient(_clusterGroup, configs.originals());

      // Instantiate the key serializer if necessary.
      _keySerializer = keySerializer != null ? keySerializer
          : configs.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
      _keySerializer.configure(configs.originals(), true);
      // Instantiate the key serializer if necessary.
      _valueSerializer = valueSerializer != null ? valueSerializer
          : configs.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
      _valueSerializer.configure(configs.originals(), false);

      // prepare to handle large messages.
      _largeMessageEnabled = configs.getBoolean(LiKafkaProducerConfig.LARGE_MESSAGE_ENABLED_CONFIG);
      _maxMessageSegmentSize = Math.min(configs.getInt(LiKafkaProducerConfig.MAX_MESSAGE_SEGMENT_BYTES_CONFIG),
          configs.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
      Serializer<LargeMessageSegment> segmentSerializer = largeMessageSegmentSerializer != null ? largeMessageSegmentSerializer
          : configs.getConfiguredInstance(LiKafkaProducerConfig.SEGMENT_SERIALIZER_CLASS_CONFIG, Serializer.class);
      segmentSerializer.configure(configs.originals(), false);
      _uuidFactory = configs.getConfiguredInstance(LiKafkaProducerConfig.UUID_FACTORY_CLASS_CONFIG, UUIDFactory.class);
      _messageSplitter = new MessageSplitterImpl(_maxMessageSegmentSize, segmentSerializer, _uuidFactory);
      _largeMessageSegmentWrappingRequired =
          configs.getBoolean(LiKafkaProducerConfig.LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_CONFIG);
    } catch (Exception e) {
      try {
        if (_mdsClient != null) {
          _mdsClient.close();
        }
      } catch (Exception e2) {
        LOG.error("Failed to close the metadata service client", e2);
      }
      throw e;
    }
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
    return send(producerRecord, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
    String topic = producerRecord.topic();
    PerClusterProducerContext context = getOrCreateProducerContextForTopic(topic);
    Producer<byte[], byte[]> producer = context.producer();
    Auditor<K, V> auditor = context.auditor();
    LongAdder numThreadsInSend = context.numThreadsInSend();

    numThreadsInSend.increment();
    boolean failed = true;
    try {
      if (context.closed()) {
        throw new IllegalStateException("LiKafkaProducer has been closed.");
      }

      K key = producerRecord.key();
      V value = producerRecord.value();
      Object auditToken = auditor.auditToken(key, value);
      Long timestamp = producerRecord.timestamp() == null ? System.currentTimeMillis() : producerRecord.timestamp();
      Integer partition = producerRecord.partition();
      Future<RecordMetadata> future = null;
      UUID messageId = _uuidFactory.getUuid(producerRecord);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Sending event: [{}, {}] with key {} to kafka topic {} to cluster {}",
            messageId.toString().replaceAll("-", ""),
            value.toString(),
            (key != null) ? key.toString() : "[none]",
            topic,
            context.cluster().name());
      }
      byte[] serializedValue;
      byte[] serializedKey;
      try {
        serializedValue = _valueSerializer.serialize(topic, value);
        serializedKey = _keySerializer.serialize(topic, key);
      } catch (Throwable t) {
        // Audit the attempt. The failure will be recorded in the finally block below.
        auditor.record(auditToken, topic, timestamp, 1L, 0L, AuditType.ATTEMPT);
        throw t;
      }
      int serializedKeyLength = serializedKey == null ? 0 : serializedKey.length;
      int serializedValueLength = serializedValue == null ? 0 : serializedValue.length;
      int sizeInBytes = serializedKeyLength + serializedValueLength;
      // Audit the attempt.
      auditor.record(auditToken, topic, timestamp, 1L, (long) sizeInBytes, AuditType.ATTEMPT);
      // We wrap the user callback for error logging and auditing purpose.
      Callback errorLoggingCallback =
          new ErrorLoggingCallback<>(messageId, auditToken, topic, timestamp, sizeInBytes, auditor, callback);
      if (_largeMessageEnabled && serializedValueLength > _maxMessageSegmentSize) {
        // Split the payload into large message segments
        List<ProducerRecord<byte[], byte[]>> segmentRecords =
            _messageSplitter.split(topic, partition, timestamp, messageId, serializedKey, serializedValue);
        Callback largeMessageCallback = new LargeMessageCallback(segmentRecords.size(), errorLoggingCallback);
        for (ProducerRecord<byte[], byte[]> segmentRecord : segmentRecords) {
          future = producer.send(segmentRecord, largeMessageCallback);
        }
      } else if (_largeMessageEnabled && _largeMessageSegmentWrappingRequired) {
        // Wrap the paylod with a large message segment, even if the payload is not big enough to split
        List<ProducerRecord<byte[], byte[]>> wrappedRecord =
            _messageSplitter.split(topic, partition, timestamp, messageId, serializedKey, serializedValue,
                serializedValueLength);
        if (wrappedRecord.size() != 1) {
          throw new IllegalStateException("Failed to create a large message segment wrapped message");
        }
        future = producer.send(wrappedRecord.get(0), errorLoggingCallback);
      } else {
        // Do not wrap with a large message segment
        future = producer.send(new ProducerRecord(topic, partition, timestamp, serializedKey, serializedValue),
            errorLoggingCallback);
      }
      failed = false;
      return future;
    } catch (SkippableException e) {
      LOG.warn("Exception thrown when producing message to partition {}-{}: {}", producerRecord.topic(),
          producerRecord.partition(), e);
      return null;
    } finally {
      numThreadsInSend.decrement();
      if (context.closed()) {
        synchronized (numThreadsInSend) {
          numThreadsInSend.notifyAll();
        }
      }
      if (failed) {
        auditor.record(auditor.auditToken(producerRecord.key(), producerRecord.value()), producerRecord.topic(),
            producerRecord.timestamp(), 1L, 0L, AuditType.FAILURE);
      }
    }
  }

  @Override
  public void flush() {
    flush(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  public void flush(long timeout, TimeUnit timeUnit) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    PerClusterProducerContext<K, V> context = getOrCreateProducerContextForTopic(topic);
    return context.producer().partitionsFor(topic);
  }

  @Override
  public Map<String, List<PartitionInfo>> partitionsFor(Set<String> topics) {
    // TODO: come back here when upstream API settles
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close() {
    close(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
    if (_producers.isEmpty()) {
      LOG.warn("No producers to close for cluster group {}", _clusterGroup);
      return;
    }

    LOG.info("Closing LiKafkaProducer for cluster group {} in {} {}...", _clusterGroup, timeout, timeUnit);

    long startTimeMs = System.currentTimeMillis();
    long budgetMs = timeUnit.toMillis(timeout);
    long deadlineTimeMs = startTimeMs + budgetMs;

    CountDownLatch countDownLatch = new CountDownLatch(_producers.entrySet().size());
    for (Map.Entry<ClusterDescriptor, PerClusterProducerContext<K, V>> entry : _producers.entrySet()) {
      Thread taskThread = new Thread(new CloseTask(entry.getValue(), deadlineTimeMs, countDownLatch));
      taskThread.start();
    }

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      throw new KafkaException("Fail to close all producers for cluster group " + _clusterGroup, e);
    }

    LOG.info("LiKafkaProducer close for cluster group {} complete in {} milliseconds", _clusterGroup,
        (System.currentTimeMillis() - startTimeMs));
  }

  private class CloseTask implements Runnable {
    private PerClusterProducerContext<K, V> _context;
    private long _deadlineTimeMs;
    private CountDownLatch _countDownLatch;

    CloseTask(PerClusterProducerContext<K, V> context, long deadlineTimeMs, CountDownLatch countDownLatch) {
      _context = context;
      _deadlineTimeMs = _deadlineTimeMs;
      _countDownLatch = countDownLatch;
    }

    public void run() {
      try {
        LOG.info("Closing LiKafkaProducer for cluster {}", _context.cluster());
        LongAdder numThreadsInSend = _context.numThreadsInSend();

        _context.setClosed(true);
        synchronized (numThreadsInSend) {
          long remainingMs = _deadlineTimeMs - System.currentTimeMillis();
          while (numThreadsInSend.intValue() > 0 && remainingMs > 0) {
            try {
              numThreadsInSend.wait(remainingMs);
            } catch (InterruptedException e) {
              LOG.error("Interrupted when there are still {} sender threads for cluster {}.",
                  numThreadsInSend.intValue(), _context.cluster());
              break;
            }
            remainingMs = _deadlineTimeMs - System.currentTimeMillis();
          }
        }

        _context.auditor().close(Math.max(0, _deadlineTimeMs - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        _context.producer().close(Math.max(0, _deadlineTimeMs - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        LOG.info("LiKafkaProducer close complete for cluster {}", _context.cluster());
      } finally {
        _countDownLatch.countDown();
      }
    }
  }

  // Transactions are not supported in federated clients.
  @Override
  public void initTransactions() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
      String consumerGroupId) throws ProducerFencedException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException("Not supported");
  }

  public Producer<byte[], byte[]> getPerClusterProducer(ClusterDescriptor cluster) {
    PerClusterProducerContext<K, V> context = _producers.get(cluster);
    return context == null ? null : context.producer();
  }

  private PerClusterProducerContext<K, V> getOrCreateProducerContextForTopic(String topic) {
    return getOrCreatePerClusterProducer(_mdsClient.getClusterForTopic(_clientId, topic));
  }

  private PerClusterProducerContext<K, V> getOrCreatePerClusterProducer(ClusterDescriptor cluster) {
    if (_producers.containsKey(cluster)) {
      return _producers.get(cluster);
    }

    // Create per-cluster producer config with the actual bootstrap URL of the physical cluster to connect to.
    Map<String, Object> configMap = _commonProducerConfigs.originals();
    configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapURL());
    LiKafkaProducerConfig perClusterProducerConfig = new LiKafkaProducerConfig(configMap);
    _producerBuilder.setProducerConfig(perClusterProducerConfig);
    PerClusterProducerContext newContext = new PerClusterProducerContext(cluster, perClusterProducerConfig,
        _producerBuilder.build());
    _producers.put(cluster, newContext);
    return newContext;
  }

  private static class ErrorLoggingCallback<K, V> implements Callback {
    private final UUID _messageId;
    private final String _topic;
    private final Long _timestamp;
    private final Integer _serializedSize;
    private final Object _auditToken;
    private final Auditor<K, V> _auditor;
    private final Callback _userCallback;

    public ErrorLoggingCallback(UUID messageId, Object auditToken, String topic, Long timestamp, Integer serializedSize,
        Auditor<K, V> auditor, Callback userCallback) {
      _messageId = messageId;
      _topic = topic;
      _timestamp = timestamp;
      _serializedSize = serializedSize;
      _auditor = auditor;
      _auditToken = auditToken;
      _userCallback = userCallback;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        LOG.error(String.format("Unable to send event %s with message id %s to kafka topic %s",
            _auditToken == null ? "[No Custom Info]" : _auditToken,
            (_messageId != null) ? _messageId.toString().replaceAll("-", "") : "[none]", _topic), e);
        // Audit the failure.
        _auditor.record(_auditToken, _topic, _timestamp, 1L, _serializedSize.longValue(), AuditType.FAILURE);
      } else {
        // Audit the success.
        _auditor.record(_auditToken, _topic, _timestamp, 1L, _serializedSize.longValue(), AuditType.SUCCESS);
      }
      if (_userCallback != null) {
        _userCallback.onCompletion(recordMetadata, e);
      }
    }
  }
}
