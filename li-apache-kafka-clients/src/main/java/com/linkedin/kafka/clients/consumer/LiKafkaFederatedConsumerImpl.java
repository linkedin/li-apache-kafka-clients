/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
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
  private ConcurrentSkipListMap<ClusterDescriptor, LiKafkaConsumer<K, V>> _consumers;

  // Consumer builder for creating per-cluster LiKafkaConsumer
  private LiKafkaConsumerBuilder<K, V> _consumerBuilder;

  // Consumer configs common to all clusters
  private LiKafkaConsumerConfig _commonConsumerConfigs;

  // max.poll.records for the federated consumer
  private int _maxPollRecordsForFederatedConsumer;

  // The number of clusters in this cluster group to connect to for the current assignment/subscription
  private int _numClustersToConnectTo;

  private ClusterDescriptor _clusterToStartPollingFrom;

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

    _consumers = new ConcurrentSkipListMap<ClusterDescriptor, LiKafkaConsumer<K, V>>(
        new Comparator<ClusterDescriptor>() {
          public int compare(ClusterDescriptor cluster1, ClusterDescriptor cluster2) {
            return cluster1.toString().compareTo(cluster2.toString());
          }
        }
    );
    _consumerBuilder = consumerBuilder != null ? consumerBuilder : new LiKafkaConsumerBuilder<K, V>();

    _mdsRequestTimeoutMs = configs.getInt(LiKafkaConsumerConfig.METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG);
    _maxPollRecordsForFederatedConsumer = configs.getInt(LiKafkaConsumerConfig.MAX_POLL_RECORDS_CONFIG);
    _clusterToStartPollingFrom = null;

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

    // If partitions are empty, it should be treated the same as unsubscribe(). This is the vanilla Kafka consumer
    // behavior.
    if (partitions.isEmpty()) {
      unsubscribe();
      return;
    }

    Map<TopicPartition, ClusterDescriptor> topicPartitionToClusterMap =
        _mdsClient.getClustersForTopicPartitions(_clientId, partitions, _mdsRequestTimeoutMs);

    // Reverse the map so that we can have per-cluster topic partition sets.
    Map<ClusterDescriptor, Set<TopicPartition>> clusterToTopicPartitionsMap = new HashMap<>();
    Set<TopicPartition> nonexistentTopicPartitions = new HashSet<>();
    for (Map.Entry<TopicPartition, ClusterDescriptor> entry : topicPartitionToClusterMap.entrySet()) {
      TopicPartition topicPartition = entry.getKey();
      ClusterDescriptor cluster = entry.getValue();
      if (cluster == null) {
        nonexistentTopicPartitions.add(topicPartition);
      } else {
        clusterToTopicPartitionsMap.computeIfAbsent(cluster, k -> new HashSet<TopicPartition>()).add(topicPartition);
      }
    }

    if (!nonexistentTopicPartitions.isEmpty()) {
      throw new IllegalStateException("Cannot assign nonexistent partitions: " + nonexistentTopicPartitions);
    }

    // This assignment should replace the previous assignment. Since max.poll.records should be reset for consumers
    // for the new assignment, close all existing consumers first.
    if (!_consumers.isEmpty()) {
      LOG.info("Closing all existing LiKafkaConsumers due to assignment change");
      close();
    }
    _numClustersToConnectTo = clusterToTopicPartitionsMap.size();
    _clusterToStartPollingFrom = null;
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
    if (_consumers.isEmpty()) {
      return ConsumerRecords.empty();
    }

    long deadlineMs = System.currentTimeMillis() + timeout;

    if (_clusterToStartPollingFrom == null) {
      _clusterToStartPollingFrom = _consumers.firstKey();
    }

    // Construct a list where the all map entries whose keys >= _clusterToStartPollingFrom appear first, and then
    // entries whose keys < _clusterToStartPollingFrom.
    ConcurrentNavigableMap<ClusterDescriptor, LiKafkaConsumer<K, V>> tailMap =
        _consumers.tailMap(_clusterToStartPollingFrom);
    ConcurrentNavigableMap<ClusterDescriptor, LiKafkaConsumer<K, V>> headMap =
        _consumers.headMap(_clusterToStartPollingFrom);
    List<Map.Entry<ClusterDescriptor, LiKafkaConsumer<K, V>>> entryList = new ArrayList<>(tailMap.entrySet());
    entryList.addAll(headMap.entrySet());

    Map<TopicPartition, List<ConsumerRecord<K, V>>> aggregatedConsumerRecords = new HashMap<>();
    int numRecordsReceived = 0;
    ClusterDescriptor lastClusterPolled = null;
    for (Map.Entry<ClusterDescriptor, LiKafkaConsumer<K, V>> entry : entryList) {
      ClusterDescriptor cluster = entry.getKey();
      LiKafkaConsumer<K, V> consumer = entry.getValue();

      // For the last consumer in the list, poll with the remaining timeout. Otherwise, poll with a zero timeout.
      ConsumerRecords<K, V> pollResult;
      if (entry == entryList.get(entryList.size() - 1)) {
        pollResult = consumer.poll(Math.max(deadlineMs - System.currentTimeMillis(), 0));
      } else {
        pollResult = consumer.poll(0);
      }

      for (TopicPartition partition : pollResult.partitions()) {
        // There should be no overlap between topic partitions returned from different clusters.
        if (aggregatedConsumerRecords.containsKey(partition)) {
          throw new IllegalStateException("Duplicate topic partition " + partition + " returned from cluster " +
              cluster + " in group " + _clusterGroup);
        }
        aggregatedConsumerRecords.put(partition, pollResult.records(partition));
      }
      numRecordsReceived += pollResult.count();
      lastClusterPolled = cluster;

      // If we have received the maximum number of records allowed for this federated consumer (in case where the number
      // of clusters in the group >= max.poll.records for the federated consumer) or the timeout has already passed
      // before finishing the iteration, stop here.
      if (numRecordsReceived == _maxPollRecordsForFederatedConsumer || System.currentTimeMillis() > deadlineMs) {
        break;
      }
    }

    _clusterToStartPollingFrom =
        _consumers.higherKey(lastClusterPolled) == null ? _consumers.firstKey() : _consumers.higherKey(lastClusterPolled);
    return new ConsumerRecords<K, V>(aggregatedConsumerRecords);
  }

  @Override
  public ConsumerRecords<K, V> poll(Duration timeout) {
    return poll(timeout.toMillis());
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
    close(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
    if (_consumers.isEmpty()) {
      LOG.warn("No consumers to close for cluster group {}", _clusterGroup);
      return;
    }

    LOG.info("Closing LiKafkaConsumer for cluster group {} in {} {}...", _clusterGroup, timeout, timeUnit);

    long startTimeMs = System.currentTimeMillis();
    long deadlineTimeMs = startTimeMs + timeUnit.toMillis(timeout);
    CountDownLatch countDownLatch = new CountDownLatch(_consumers.entrySet().size());
    for (LiKafkaConsumer<K, V> consumer : _consumers.values()) {
      new Thread(() -> {
        try {
          consumer.close(deadlineTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        } finally {
          countDownLatch.countDown();
        }
      }).start();
    }

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      throw new KafkaException("Fail to close all consumers for cluster group " + _clusterGroup, e);
    }

    LOG.info("LiKafkaConsumer close for cluster group {} complete in {} milliseconds", _clusterGroup,
        (System.currentTimeMillis() - startTimeMs));
  }

  @Override
  public void close(Duration timeout) {
    close(timeout.toMillis(), TimeUnit.MILLISECONDS);
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

    // Create per-cluster consumer config where the following cluster-specific properties:
    //   - bootstrap.server - the actual bootstrap URL of the physical cluster to connect to
    //   - max.poll.records - the property value set for the federated consumer / the number of clusters in this cluster group
    //                        if the federated consumer property < the number of clusters in the group, set it to 1
    //                        (poll() will make sure that it won't fetch more than the federated consumer property)
    Map<String, Object> configMap = _commonConsumerConfigs.originals();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapURL());
    int maxPollRecordsPerCluster = Math.max(_maxPollRecordsForFederatedConsumer / _numClustersToConnectTo, 1);
    configMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecordsPerCluster);
    _consumerBuilder.setConsumerConfig(configMap);
    LiKafkaConsumer<K, V> newConsumer = _consumerBuilder.build();
    _consumers.put(cluster, newConsumer);
    return newConsumer;
  }
}