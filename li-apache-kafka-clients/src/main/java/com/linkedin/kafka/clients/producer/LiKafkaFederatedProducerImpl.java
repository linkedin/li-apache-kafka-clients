/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClient;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a producer implementation that works with a federated Kafka cluster, which consists of one or more physical
 * Kafka clusters.
 */
public class LiKafkaFederatedProducerImpl<K, V> implements LiKafkaProducer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaFederatedProducerImpl.class);

  // The cluster group this client is talking to
  private ClusterGroupDescriptor _clusterGroup;

  // The client for the metadata service which serves cluster and topic metadata
  private MetadataServiceClient _mdsClient;

  // Timeout in milliseconds for metadata service requests.
  private int _mdsRequestTimeoutMs;

  // The id of this client assigned by the metadata service
  private UUID _clientId;

  // Per cluster producers
  private Map<ClusterDescriptor, LiKafkaProducer<K, V>> _producers;

  // Producer builder for creating per-cluster LiKafkaProducer
  private LiKafkaProducerBuilder<K, V> _producerBuilder;

  // Producer configs common to all clusters
  private LiKafkaProducerConfig _commonProducerConfigs;

  public LiKafkaFederatedProducerImpl(Properties props) {
    this(new LiKafkaProducerConfig(props), null, null);
  }

  public LiKafkaFederatedProducerImpl(Properties props, MetadataServiceClient mdsClient,
      LiKafkaProducerBuilder<K, V> producerBuilder) {
    this(new LiKafkaProducerConfig(props), mdsClient, producerBuilder);
  }

  public LiKafkaFederatedProducerImpl(Map<String, ?> configs) {
    this(new LiKafkaProducerConfig(configs), null, null);
  }

  public LiKafkaFederatedProducerImpl(Map<String, ?> configs, MetadataServiceClient mdsClient,
      LiKafkaProducerBuilder<K, V> producerBuilder) {
    this(new LiKafkaProducerConfig(configs), mdsClient, producerBuilder);
  }

  @SuppressWarnings("unchecked")
  private LiKafkaFederatedProducerImpl(LiKafkaProducerConfig configs, MetadataServiceClient mdsClient,
      LiKafkaProducerBuilder<K, V> producerBuilder) {
    _commonProducerConfigs = configs;
    _clusterGroup = new ClusterGroupDescriptor(configs.getString(LiKafkaProducerConfig.CLUSTER_ENVIRONMENT_CONFIG),
        configs.getString(LiKafkaProducerConfig.CLUSTER_GROUP_CONFIG));

    // Each per-cluster producer and auditor will be intantiated by the passed-in producer builder when the client
    // begins to produce to that cluster. If a null builder is passed, create a default one, which builds LiKafkaProducer.
    _producers = new ConcurrentHashMap<ClusterDescriptor, LiKafkaProducer<K, V>>();
    _producerBuilder = producerBuilder != null ? producerBuilder : new LiKafkaProducerBuilder<K, V>();

    _mdsRequestTimeoutMs = configs.getInt(LiKafkaProducerConfig.METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG);

    try {
      // Instantiate metadata service client if necessary.
      _mdsClient = mdsClient != null ? mdsClient :
          configs.getConfiguredInstance(LiKafkaProducerConfig.METADATA_SERVICE_CLIENT_CLASS_CONFIG, MetadataServiceClient.class);

      // Register this federated client with the metadata service. The metadata service will assign a UUID to this
      // client, which will be used for later interaction between the metadata service and the client.
      //
      // Registration may also return further information such as the metadata server version and any protocol settings.
      // We assume that such information will be kept and used by the metadata service client itself.
      _clientId = _mdsClient.registerFederatedClient(_clusterGroup, configs.originals(), _mdsRequestTimeoutMs);
    } catch (Exception e) {
      try {
        if (_mdsClient != null) {
          _mdsClient.close(_mdsRequestTimeoutMs);
        }
      } catch (Exception e2) {
        e.addSuppressed(e2);
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
    return getOrCreateProducerForTopic(producerRecord.topic()).send(producerRecord, callback);
  }

  @Override
  public void flush() {
    flush(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  public void flush(long timeout, TimeUnit timeUnit) {
    if (_producers.isEmpty()) {
      LOG.warn("No producers to flush for cluster group {}", _clusterGroup);
      return;
    }

    LOG.info("Flushing LiKafkaProducer for cluster group {} in {} {}...", _clusterGroup, timeout, timeUnit);

    long startTimeMs = System.currentTimeMillis();
    long deadlineTimeMs = startTimeMs + timeUnit.toMillis(timeout);
    CountDownLatch countDownLatch = new CountDownLatch(_producers.entrySet().size());
    for (Map.Entry<ClusterDescriptor, LiKafkaProducer<K, V>> entry : _producers.entrySet()) {
      new Thread(() -> {
          try {
            entry.getValue().flush(deadlineTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
          } finally {
            countDownLatch.countDown();
          }
        }).start();
    }

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      throw new KafkaException("Fail to flush all producers for cluster group " + _clusterGroup, e);
    }

    LOG.info("LiKafkaProducer flush for cluster group {} complete in {} milliseconds", _clusterGroup,
        (System.currentTimeMillis() - startTimeMs));
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return getOrCreateProducerForTopic(topic).partitionsFor(topic);
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
    long deadlineTimeMs = startTimeMs + timeUnit.toMillis(timeout);
    CountDownLatch countDownLatch = new CountDownLatch(_producers.entrySet().size());
    for (Map.Entry<ClusterDescriptor, LiKafkaProducer<K, V>> entry : _producers.entrySet()) {
      new Thread(() -> {
          try {
            entry.getValue().close(deadlineTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
          } finally {
            countDownLatch.countDown();
          }
        }).start();
    }

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      throw new KafkaException("Fail to close all producers for cluster group " + _clusterGroup, e);
    }

    LOG.info("LiKafkaProducer close for cluster group {} complete in {} milliseconds", _clusterGroup,
        (System.currentTimeMillis() - startTimeMs));
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

  public LiKafkaProducer<K, V> getPerClusterProducer(ClusterDescriptor cluster) {
    return _producers.get(cluster);
  }

  private LiKafkaProducer<K, V> getOrCreateProducerForTopic(String topic) {
    return getOrCreatePerClusterProducer(_mdsClient.getClusterForTopic(_clientId, topic, _mdsRequestTimeoutMs));
  }

  private LiKafkaProducer<K, V> getOrCreatePerClusterProducer(ClusterDescriptor cluster) {
    if (_producers.containsKey(cluster)) {
      return _producers.get(cluster);
    }

    // Create per-cluster producer config with the actual bootstrap URL of the physical cluster to connect to.
    Map<String, Object> configMap = _commonProducerConfigs.originals();
    configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapURL());
    _producerBuilder.setProducerConfig(configMap);
    LiKafkaProducer<K, V> newProducer = _producerBuilder.build();
    _producers.put(cluster, newProducer);
    return newProducer;
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
