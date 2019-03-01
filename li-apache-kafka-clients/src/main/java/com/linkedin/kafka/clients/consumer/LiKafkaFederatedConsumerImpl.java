/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClient;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a consumer implementation that works with a federated Kafka cluster, which consists of one or more physical
 * Kafka clusters.
 */
public class LiKafkaFederatedConsumerImpl<K, V> implements LiKafkaConsumer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaFederatedConsumerImpl.class);

  // The cluster group this client is talking to
  private ClusterGroupDescriptor _clusterGroup;

  // The client for the metadata service which serves cluster and topic metadata
  private MetadataServiceClient _mdsClient;

  // Timeout in milliseconds for metadata service requests.
  private int _mdsRequestTimeoutMs;

  // The id of this client assigned by the metadata service
  private UUID _clientId;

  // Per cluster Consumers
  private Map<ClusterDescriptor, LiKafkaConsumer<K, V>> _consumers;

  // Consumer builder for creating per-cluster LiKafkaConsumer
  private LiKafkaConsumerBuilder<K, V> _consumerBuilder;

  // Consumer configs common to all clusters
  private LiKafkaConsumerConfig _commonConsumerConfigs;

  public LiKafkaFederatedConsumerImpl(Properties props) {
    this(new LiKafkaConsumerConfig(props), null, null);
  }

  public LiKafkaFederatedConsumerImpl(Properties props, MetadataServiceClient mdsClient, LiKafkaConsumerBuilder<K, V> consumerBuilder) {
    this(new LiKafkaConsumerConfig(props), mdsClient, consumerBuilder);
  }

  public LiKafkaFederatedConsumerImpl(Map<String, ?> configs) {
    this(new LiKafkaConsumerConfig(configs), null, null);
  }

  public LiKafkaFederatedConsumerImpl(Map<String, ?> configs, MetadataServiceClient mdsClient,
      LiKafkaConsumerBuilder<K, V> consumerBuilder) {
    this(new LiKafkaConsumerConfig(configs), mdsClient, consumerBuilder);
  }

  @SuppressWarnings("unchecked")
  private LiKafkaFederatedConsumerImpl(LiKafkaConsumerConfig configs, MetadataServiceClient mdsClient,
      LiKafkaConsumerBuilder<K, V> consumerBuilder) {
    _commonConsumerConfigs = configs;
    _clusterGroup = new ClusterGroupDescriptor(configs.getString(LiKafkaConsumerConfig.CLUSTER_ENVIRONMENT_CONFIG),
        configs.getString(LiKafkaConsumerConfig.CLUSTER_GROUP_CONFIG));

    // Each per-cluster consumer and auditor will be instantiated by the passed-in consumer builder when the client
    // begins to consume from that cluster. If a null builder is passed, create a default one, which builds
    // LiKafkaConsumer.
    _consumers = new ConcurrentHashMap<ClusterDescriptor, LiKafkaConsumer<K, V>>();
    _consumerBuilder = consumerBuilder != null ? consumerBuilder : new LiKafkaConsumerBuilder<K, V>();

    _mdsRequestTimeoutMs = configs.getInt(LiKafkaConsumerConfig.METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG);

    try {
      // Instantiate metadata service client if necessary.
      _mdsClient = mdsClient != null ? mdsClient :
          configs.getConfiguredInstance(LiKafkaConsumerConfig.METADATA_SERVICE_CLIENT_CLASS_CONFIG, MetadataServiceClient.class);

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
  public Set<TopicPartition> assignment() {
    Set<TopicPartition> aggregate = new HashSet<>();
    for (LiKafkaConsumer<K, V> consumer : _consumers.values()) {
      aggregate.addAll(consumer.assignment());
    }
    return aggregate;
  }

  @Override
  public Set<String> subscription() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void subscribe(Collection<String> topics) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    if (partitions == null) {
      throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
    }

    // If partitions are empty, it should be treated the same as unsubscribe().
    if (partitions.isEmpty()) {
      unsubscribe();
      return;
    }

    Map<TopicPartition, ClusterDescriptor> topicPartitionToClusterMap =
        _mdsClient.getClustersForTopicPartitions(_clientId, partitions, _mdsRequestTimeoutMs);

    // Reverse the map so that we can have per-cluster topic partition sets.
    Map<ClusterDescriptor, Set<TopicPartition>> clusterToTopicPartitionsMap = new HashMap<>();
    for (Map.Entry<TopicPartition, ClusterDescriptor> entry : topicPartitionToClusterMap.entrySet()) {
      TopicPartition topicPartition = entry.getKey();
      ClusterDescriptor cluster = entry.getValue();
      clusterToTopicPartitionsMap.computeIfAbsent(cluster, k -> new HashSet<TopicPartition>()).add(topicPartition);
    }

    for (Map.Entry<ClusterDescriptor, Set<TopicPartition>> entry : clusterToTopicPartitionsMap.entrySet()) {
      getOrCreatePerClusterConsumer(entry.getKey()).assign(entry.getValue());
    }
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void subscribe(Pattern pattern) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void unsubscribe() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ConsumerRecords<K, V> poll(long timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ConsumerRecords<K, V> poll(Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void commitSync() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void commitSync(Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void commitAsync() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void seekToCommitted(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public long position(TopicPartition partition) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public long position(TopicPartition partition, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Long committedSafeOffset(TopicPartition tp) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Set<TopicPartition> paused() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Long safeOffset(TopicPartition tp, long messageOffset) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Long safeOffset(TopicPartition tp) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<TopicPartition, Long> safeOffsets() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close(Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void wakeup() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  // Intended for testing only
  LiKafkaConsumer<K, V> getPerClusterConsumer(ClusterDescriptor cluster) {
    return _consumers.get(cluster);
  }

  private LiKafkaConsumer<K, V> getOrCreateConsumerForTopic(String topic) {
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("Topic cannot be null or empty");
    }

    // TODO: Handle nonexistent topics more elegantly with auto topic creation option
    ClusterDescriptor cluster = _mdsClient.getClusterForTopic(_clientId, topic, _mdsRequestTimeoutMs);
    if (cluster == null) {
      throw new IllegalStateException("Topic " + topic + " not found in the metadata service");
    }

    return getOrCreatePerClusterConsumer(_mdsClient.getClusterForTopic(_clientId, topic, _mdsRequestTimeoutMs));
  }

  // Returns null if the specified topic does not exist in the cluster group.
  private LiKafkaConsumer<K, V> getOrCreatePerClusterConsumer(ClusterDescriptor cluster) {
    if (cluster == null) {
      throw new IllegalArgumentException("Cluster cannot be null");
    }

    if (_consumers.containsKey(cluster)) {
      return _consumers.get(cluster);
    }

    // Create per-cluster consumer config with the actual bootstrap URL of the physical cluster to connect to.
    Map<String, Object> configMap = _commonConsumerConfigs.originals();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapURL());
    _consumerBuilder.setConsumerConfig(configMap);
    LiKafkaConsumer<K, V> newConsumer = _consumerBuilder.build();
    _consumers.put(cluster, newConsumer);
    return newConsumer;
  }
}