/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import com.linkedin.kafka.clients.largemessage.MessageSplitter;
import com.linkedin.kafka.clients.largemessage.MessageSplitterImpl;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClient;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
    private Producer<byte[], byte[]> _producer;
    /*package private for testing*/ Auditor<K, V> _auditor;

    // A counter of the threads in the middle of sending messages. This is needed to ensure when we close the producer
    // everything is audited.
    private final LongAdder _numThreadsInSend = new LongAdder();
    private volatile boolean _closed;

    // This is null if the underlying producer does not have an implementation for time-bounded flush
    private final Method _boundedFlushMethod;
    private final LongAdder _boundedFlushThreadCount = new LongAdder();

    public PerClusterProducerContext(LiKafkaProducerConfig configs) {
      _producer = new KafkaProducer<>(configs.originals(), new ByteArraySerializer(), new ByteArraySerializer());

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
  }

  // The cluster group this client is talking to
  private ClusterGroupDescriptor _clusterGroup;

  // The client for the metadata service which serves cluster and topic metadata
  private MetadataServiceClient _mdsClient;

  // The id of this client assigned by the metadata service
  private UUID _clientId;

  // Per cluster producers, which sends raw bytes
  private Map<ClusterDescriptor, PerClusterProducerContext<K, V>> _producers;

  // Large message settings
  private final boolean _largeMessageEnabled;
  private final int _maxMessageSegmentSize;
  private final MessageSplitter _messageSplitter;

  // Serializers
  private Serializer<K> _keySerializer;
  private Serializer<V> _valueSerializer;

  private final UUIDFactory<K, V> _uuidFactory;

  public LiKafkaFederatedProducerImpl(Properties props) {
    this(new LiKafkaProducerConfig(props), null, null, null, null);
  }

  public LiKafkaFederatedProducerImpl(Properties props,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Serializer<LargeMessageSegment> largeMessageSegmentSerializer,
      MetadataServiceClient mdsClient) {
    this(new LiKafkaProducerConfig(props), keySerializer, valueSerializer, largeMessageSegmentSerializer, mdsClient);
  }

  public LiKafkaFederatedProducerImpl(Map<String, ?> configs) {
    this(new LiKafkaProducerConfig(configs), null, null, null, null);
  }

  public LiKafkaFederatedProducerImpl(Map<String, ?> configs,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Serializer<LargeMessageSegment> largeMessageSegmentSerializer,
      MetadataServiceClient mdsClient) {
    this(new LiKafkaProducerConfig(configs), keySerializer, valueSerializer, largeMessageSegmentSerializer, mdsClient);
  }

  @SuppressWarnings("unchecked")
  private LiKafkaFederatedProducerImpl(LiKafkaProducerConfig configs,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Serializer<LargeMessageSegment> largeMessageSegmentSerializer,
      MetadataServiceClient mdsClient) {
    _clusterGroup = new ClusterGroupDescriptor(configs.getString(LiKafkaProducerConfig.CLUSTER_ENVIRONMENT_CONFIG),
        configs.getString(LiKafkaProducerConfig.CLUSTER_GROUP_CONFIG));

    // Each per-cluster producer and auditor will be intantiated when the client starts producing to that cluster.
    _producers = new HashMap<ClusterDescriptor, PerClusterProducerContext<K, V>>();

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
    throw new UnsupportedOperationException("Not implemented yet");
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
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<String, List<PartitionInfo>> partitionsFor(Set<String> topics) {
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
    throw new UnsupportedOperationException("Not implemented yet");
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
}