/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.common.LiKafkaFederatedClient;
import com.linkedin.kafka.clients.common.LiKafkaFederatedClientType;
import com.linkedin.kafka.clients.common.PartitionKeyedMapLookupResult;
import com.linkedin.kafka.clients.common.PartitionLookupResult;
import com.linkedin.kafka.clients.common.TopicLookupResult;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClient;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClientException;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.mario.common.websockets.MessageType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a consumer implementation that works with a federated Kafka cluster, which consists of one or more physical
 * Kafka clusters. This class is not thread-safe.
 */
public class LiKafkaFederatedConsumerImpl<K, V> implements LiKafkaConsumer<K, V>, LiKafkaFederatedClient {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaFederatedConsumerImpl.class);
  private static final Duration CONSUMER_CLOSE_MAX_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration RELOAD_CONFIG_EXECUTION_TIMEOUT = Duration.ofMinutes(10);
  private static final AtomicInteger FEDERATED_CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

  // The cluster group this client is talking to
  private ClusterGroupDescriptor _clusterGroup;

  // The client for the metadata service which serves cluster and topic metadata
  private MetadataServiceClient _mdsClient;

  // Timeout in milliseconds for metadata service requests.
  private int _mdsRequestTimeoutMs;

  private class ClusterConsumerPair<K, V> {
    private final ClusterDescriptor _cluster;
    private final LiKafkaConsumer<K, V> _consumer;

    public ClusterConsumerPair(ClusterDescriptor cluster, LiKafkaConsumer<K, V> consumer) {
      _cluster = cluster;
      _consumer = consumer;
    }

    public ClusterDescriptor getCluster() {
      return _cluster;
    }

    public LiKafkaConsumer<K, V> getConsumer() {
      return _consumer;
    }
  }

  // Per cluster Consumers
  private volatile List<ClusterConsumerPair<K, V>> _consumers;

  // Consumer builder for creating per-cluster LiKafkaConsumer
  private LiKafkaConsumerBuilder<K, V> _consumerBuilder;

  // Consumer configs common to all clusters
  private LiKafkaConsumerConfig _commonConsumerConfigs;

  // Consumer configs received from Conductor at boot up time
  private Map<String, String> _bootupConfigsFromConductor;

  // max.poll.records for the federated consumer
  private int _maxPollRecordsForFederatedConsumer;

  // metadata.max.age.ms, also used as the interval for polling the creation of nonexistent topics that are part of the
  // current assignment/subscription
  private int _metadataMaxAgeMs;

  // The default timeout for blocking calls specified by default.api.timeout.ms consumer properties.
  // This is exposed for federated consumer methods with the default timeout that need to invoke the individual
  // consumer's corresponding methods concurrently.
  private Duration _defaultApiTimeout;

  // retry.backoff.ms
  private long _retryBackoffMs;

  // The number of clusters in this cluster group to connect to for the current assignment/subscription
  private int _numClustersToConnectTo;

  private int _nextClusterIndexToPoll;

  // Number of consumers that successfully applied configs at boot up time, used for testing only
  private Set<LiKafkaConsumer<K, V>> _numConsumersWithBootupConfigs = new HashSet<>();

  // The prefix of the client.id property to be used for individual consumers. Since the client id is part of the
  // consumer metric keys, we need to use cluster-specific client ids to differentiate metrics from different clusters.
  // Per-cluster client id is generated by appending the cluster name to this prefix.
  //
  // If user set the client.id property for the federated client, it will be used as the prefix. If not, a new prefix
  // is generated.
  private String _clientIdPrefix;

  private volatile boolean _closed;

  // Number of config reload operations executed
  private volatile int _numConfigReloads;

  // This contains the result of the location lookup for a set of topic partitions across multiple clusters in a cluster
  // group.
  private class PartitionsByClusterResult {
    private Map<ClusterDescriptor, Collection<TopicPartition>> _clusterToPartitionsMap;
    private Set<String> _nonexistentTopics;

    public PartitionsByClusterResult() {
      _clusterToPartitionsMap = Collections.emptyMap();
      _nonexistentTopics = Collections.emptySet();
    }

    public PartitionsByClusterResult(Map<ClusterDescriptor, Collection<TopicPartition>> clusterToPartitionsMap,
        Set<String> nonexistentTopics) {
      _clusterToPartitionsMap = clusterToPartitionsMap;
      _nonexistentTopics = nonexistentTopics;
    }

    public Map<ClusterDescriptor, Collection<TopicPartition>> getClusterToPartitionsMap() {
      return _clusterToPartitionsMap;
    }

    public Set<String> getNonexistentTopics() {
      return _nonexistentTopics;
    }
  }

  // Current assignment/subscription state at the federated level
  private FederatedSubscriptionState _currentSubscription;

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
    _clusterGroup = new ClusterGroupDescriptor(configs.getString(LiKafkaConsumerConfig.CLUSTER_GROUP_CONFIG),
        configs.getString(LiKafkaConsumerConfig.CLUSTER_ENVIRONMENT_CONFIG));

    // Each per-cluster consumer and auditor will be instantiated by the passed-in consumer builder when the client
    // begins to consume from that cluster. If a null builder is passed, create a default one, which builds
    // LiKafkaConsumer.

    _consumers = new ArrayList<>();
    _consumerBuilder = consumerBuilder != null ? consumerBuilder : new LiKafkaConsumerBuilder<K, V>();

    _mdsRequestTimeoutMs = configs.getInt(LiKafkaConsumerConfig.METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG);
    _maxPollRecordsForFederatedConsumer = configs.getInt(LiKafkaConsumerConfig.MAX_POLL_RECORDS_CONFIG);
    _metadataMaxAgeMs = configs.getInt(ConsumerConfig.METADATA_MAX_AGE_CONFIG);
    _defaultApiTimeout = Duration.ofMillis(configs.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
    _retryBackoffMs = configs.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

    _currentSubscription = Unsubscribed.getInstance();
    _nextClusterIndexToPoll = 0;

    String clientIdPrefix = (String) configs.originals().get(ConsumerConfig.CLIENT_ID_CONFIG);
    if (clientIdPrefix == null || clientIdPrefix.length() == 0) {
      clientIdPrefix = "consumer-" + FEDERATED_CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
    }
    _clientIdPrefix = clientIdPrefix;

    _closed = false;
    _numConfigReloads = 0;

    // Instantiate metadata service client if necessary.
    _mdsClient = mdsClient != null ? mdsClient :
        configs.getConfiguredInstance(LiKafkaConsumerConfig.METADATA_SERVICE_CLIENT_CLASS_CONFIG, MetadataServiceClient.class);

    // Register this federated client with the metadata service. The metadata service will assign a UUID to this
    // client, which will be used for later interaction between the metadata service and the client.
    //
    // Registration may also return further information such as the metadata server version and any protocol settings.
    // We assume that such information will be kept and used by the metadata service client itself.
    //
    _mdsClient.registerFederatedClient(this, _clusterGroup, configs.originals(), _mdsRequestTimeoutMs);

    // Create a watchdog thread that polls the creation of nonexistent topics in the current assignment/subscription
    // and re-assign/subscribe if any of them have been created since the last poll.
    ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();
    Thread t = new Thread(() -> {
      refreshSubscriptionMetadata();
    });
    t.setDaemon(true);
    t.setName("LiKafkaConsumer-refresh-subscription-metadata");
    t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        throw new KafkaException("Thread " + t.getName() + " throws exception", e);
      }
    });
    es.scheduleAtFixedRate(t, 0, _metadataMaxAgeMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public LiKafkaFederatedClientType getClientType() {
    return LiKafkaFederatedClientType.FEDERATED_CONSUMER;
  }

  @Override
  synchronized public Set<TopicPartition> assignment() {
    ensureOpen();
    switch (_currentSubscription.getSubscriptionType()) {
      case USER_ASSIGNED:
        return ((UserAssigned) _currentSubscription).getAssignment();

      case AUTO_TOPICS:
      case AUTO_PATTERN: {
        // Get the aggregate of currently assigned partitions from all consumers.
        Set<TopicPartition> aggregate = new HashSet<>();
        for (ClusterConsumerPair<K, V> entry : _consumers) {
          aggregate.addAll(entry.getConsumer().assignment());
        }
        return aggregate;
      }

      case NONE:
        return Collections.emptySet();

      default:
        throw new IllegalStateException("invalid subscription type: " +
            _currentSubscription.getSubscriptionType().name());
    }
  }

  @Override
  synchronized public Set<String> subscription() {
    ensureOpen();
    switch (_currentSubscription.getSubscriptionType()) {
      case AUTO_TOPICS:
        return ((Subscribed) _currentSubscription).getSubscription();

      case AUTO_PATTERN:
        // Get the aggregate of currently subscriptions from all consumers.
        Set<String> aggregate = new HashSet<>();
        for (ClusterConsumerPair<K, V> entry : _consumers) {
          aggregate.addAll(entry.getConsumer().subscription());
        }
        return aggregate;

      case USER_ASSIGNED:
        // TODO: Interestingly, vanilla consumer returns the previously used subscription in the case of USER_ASSIGNED.
        // Implement this if there is a real use case.
        return Collections.emptySet();

      case NONE:
        return Collections.emptySet();

      default:
        throw new IllegalStateException("invalid subscription type: " +
            _currentSubscription.getSubscriptionType().name());
    }
  }

  @Override
  synchronized public void subscribe(Collection<String> topics) {
    subscribe(topics, new NoOpConsumerRebalanceListener());
  }

  @Override
  synchronized public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    ensureOpen();
    if (topics == null) {
      throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
    }

    // If topics are empty, it should be treated as unsubscribe(). This is the vanilla Kafka consumer behavior.
    if (topics.isEmpty()) {
      unsubscribe();
      return;
    }

    Set<String> topicsSet = new HashSet<>(topics);
    TopicLookupResult topicLookupResult;
    try {
      topicLookupResult = _mdsClient.getClustersForTopics(topicsSet, _clusterGroup, _mdsRequestTimeoutMs);
    } catch (MetadataServiceClientException e) {
      throw new KafkaException("failed to get clusters for topics " + topics + ": ", e);
    }

    if (!topicLookupResult.getNonexistentTopics().isEmpty()) {
      LOG.warn("the following topics in the requested subscription currently do not exist: {}",
          topicLookupResult.getNonexistentTopics());
    }
    _currentSubscription = new Subscribed(topicsSet, topicLookupResult.getNonexistentTopics());

    // This subscription should replace the previous assignment/subscription. Since max.poll.records should be reset for
    // consumers for the new subscription, close all existing consumers first.
    if (!_consumers.isEmpty()) {
      LOG.debug("closing all existing LiKafkaConsumers due to subscription change");
      closeNoLock(_defaultApiTimeout);
      _consumers.clear();
    }

    Map<ClusterDescriptor, Set<String>> topicsByCluster = topicLookupResult.getTopicsByCluster();
    _numClustersToConnectTo = topicsByCluster.size();
    _nextClusterIndexToPoll = 0;

    for (Map.Entry<ClusterDescriptor, Set<String>> entry : topicsByCluster.entrySet()) {
      ClusterDescriptor cluster = entry.getKey();
      LiKafkaConsumer<K, V> consumer = createPerClusterConsumer(cluster);
      // TODO: create a wrapper callback that supports federation
      consumer.subscribe(entry.getValue(), callback);
      _consumers.add(new ClusterConsumerPair<K, V>(cluster, consumer));
    }
  }

  @Override
  synchronized public void assign(Collection<TopicPartition> partitions) {
    ensureOpen();
    if (partitions == null) {
      throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
    }

    // If topics are empty, it should be treated as unsubscribe(). This is the vanilla Kafka consumer behavior.
    if (partitions.isEmpty()) {
      unsubscribe();
      return;
    }

    // TODO: Throw an exception if there exists a partition whose topic exists but the specified partition number is
    // out of range for that topic. Whether of not to throw an exception can be controlled by a property.
    Set<TopicPartition> partitionsSet = new HashSet<>(partitions);
    PartitionLookupResult partitionLookupResult;
    try {
      partitionLookupResult = _mdsClient.getClustersForTopicPartitions(partitionsSet, _clusterGroup,
          _mdsRequestTimeoutMs);
    } catch (MetadataServiceClientException e) {
      throw new KafkaException("failed to get clusters for partitions " + partitions + ": ", e);
    }

    _currentSubscription = new UserAssigned(partitionsSet, partitionLookupResult.getNonexistentTopics());
    if (!partitionLookupResult.getNonexistentTopics().isEmpty()) {
      LOG.warn("the following topics in the requested assignment currently do not exist: {}",
          partitionLookupResult.getNonexistentTopics());
    }

    // This assignment should replace the previous assignment/subscription. Since max.poll.records should be reset for
    // consumers for the new assignment, close all existing consumers first.
    if (!_consumers.isEmpty()) {
      LOG.debug("closing all existing LiKafkaConsumers due to assignment change");
      closeNoLock(_defaultApiTimeout);
      _consumers.clear();
    }

    Map<ClusterDescriptor, Set<TopicPartition>> partitionsByCluster = partitionLookupResult.getPartitionsByCluster();
    _numClustersToConnectTo = partitionsByCluster.size();
    _nextClusterIndexToPoll = 0;

    for (Map.Entry<ClusterDescriptor, Set<TopicPartition>> entry : partitionsByCluster.entrySet()) {
      LiKafkaConsumer<K, V> consumer = createPerClusterConsumer(entry.getKey());
      consumer.assign(entry.getValue());
      _consumers.add(new ClusterConsumerPair<K, V>(entry.getKey(), consumer));
    }
  }

  @Override
  synchronized public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public void subscribe(Pattern pattern) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public void unsubscribe() {
    ensureOpen();
    _currentSubscription = Unsubscribed.getInstance();
    for (ClusterConsumerPair<K, V> entry : _consumers) {
      LiKafkaConsumer<K, V> consumer = entry.getConsumer();
      consumer.unsubscribe();
    }
  }

  @Override
  synchronized public ConsumerRecords<K, V> poll(long timeout) {
    ensureOpen();
    // Get an immutable copy of the current set of consumers.
    List<ClusterConsumerPair<K, V>> consumers = _consumers;

    if (consumers.isEmpty() ||
        _currentSubscription.getSubscriptionType() == FederatedSubscriptionState.SubscriptionType.NONE) {
      return ConsumerRecords.empty();
    }

    long deadlineMs = System.currentTimeMillis() + timeout;
    Map<TopicPartition, List<ConsumerRecord<K, V>>> aggregatedConsumerRecords = new HashMap<>();
    Map<TopicPartition, ClusterDescriptor> partitionToClusterMap = new HashMap<>();
    int numRecordsReceived = 0;
    int currentClusterIndex = _nextClusterIndexToPoll;
    for (int i = 0; i < consumers.size(); i++) {
      ClusterConsumerPair<K, V> entry = consumers.get(currentClusterIndex);
      ClusterDescriptor cluster = entry.getCluster();
      LiKafkaConsumer<K, V> consumer = entry.getConsumer();

      // For the last consumer in the list, poll with the remaining timeout. Otherwise, poll with a zero timeout.
      ConsumerRecords<K, V> pollResult;
      if (i == consumers.size() - 1) {
        pollResult = consumer.poll(Math.max(deadlineMs - System.currentTimeMillis(), 0));
      } else {
        pollResult = consumer.poll(0);
      }

      for (TopicPartition partition : pollResult.partitions()) {
        // There should be no overlap between topic partitions returned from different clusters.
        if (aggregatedConsumerRecords.containsKey(partition)) {
          throw new IllegalStateException(
              "Duplicate topic partition " + partition + " exists in clusters " +
                  cluster + " and " + partitionToClusterMap.get(partition).getName() + " in group " + _clusterGroup);
        }
        aggregatedConsumerRecords.put(partition, pollResult.records(partition));
        partitionToClusterMap.put(partition, cluster);
      }
      numRecordsReceived += pollResult.count();
      currentClusterIndex = (currentClusterIndex + 1) % consumers.size();

      // If we have received the maximum number of records allowed for this federated consumer (in case where the number
      // of clusters in the group >= max.poll.records for the federated consumer) or the timeout has already passed
      // before finishing the iteration, stop here.
      if (numRecordsReceived == _maxPollRecordsForFederatedConsumer || System.currentTimeMillis() > deadlineMs) {
        break;
      }
    }

    _nextClusterIndexToPoll = (_nextClusterIndexToPoll + 1) % consumers.size();

    return new ConsumerRecords<K, V>(aggregatedConsumerRecords);
  }

  @Override
  public ConsumerRecords<K, V> poll(Duration timeout) {
    return poll(timeout.toMillis());
  }

  @Override
  synchronized public void commitSync() {
    commitSync(_defaultApiTimeout);
  }

  @Override
  synchronized public void commitSync(Duration timeout) {
    executePerClusterCallback("commitSync", null /* call for all consumers */,
        (consumer, ignored, remainingTimeout) -> consumer.commitSync(remainingTimeout), timeout);
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    commitSync(offsets, _defaultApiTimeout);
  }

  // ATTN: UnknownTopicOrPartitionException may be received - this is a retriable exception..
  // if not resolved by the time, timeout exception
  @Override
  synchronized public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    // Vanilla consumer's commitSync() retries on nonexistent partitions until the given timeout is reached. Since
    // federated consumer does not know where the currently nonexistent topics in the input will be created, it mimics
    // the vanilla consumer's behavior by retrying after waiting retry.backoff.ms until the given timeout is reached.
    long now = System.currentTimeMillis();
    long deadlineTimeMs = now + timeout.toMillis();
    while (now < deadlineTimeMs) {
      PartitionKeyedMapLookupResult offsetsByClusterResult = getPartitionKeyedMapsByCluster(offsets);
      // Commit offset with existing partitions first.
      executePerClusterCallback("commitSync", offsetsByClusterResult.getPartitionKeyedMapsByCluster(),
          (consumer, perClusterOffsets, remainingTimeout) -> {
            consumer.commitSync((Map<TopicPartition, OffsetAndMetadata>) perClusterOffsets, remainingTimeout);
          }, Duration.ofMillis(Math.min(deadlineTimeMs - System.currentTimeMillis(), 0)));

      if (offsetsByClusterResult.getNonexistentTopics().isEmpty()) {
        return;
      }

      // There are nonexistent topics in the given offsets. Sleep and retry;
      try {
        Thread.sleep(Math.min(deadlineTimeMs - System.currentTimeMillis(), _retryBackoffMs));
      } catch (InterruptedException e) {
        LOG.warn("interrupted while retrying commitSync()");
        throw new InterruptException(e);
      }

      now = System.currentTimeMillis();
    }
    throw new TimeoutException("Failed to complete commitSync within " + timeout);
  }

  @Override
  public void commitAsync() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public void commitAsync(OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public void seek(TopicPartition partition, long offset) {
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
  synchronized public void seekToCommitted(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public long position(TopicPartition partition) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public long position(TopicPartition partition, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public OffsetAndMetadata committed(TopicPartition partition) {

    return committed(partition, _defaultApiTimeout);
  }

  @Override
  synchronized public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
    if (partition == null) {
      throw new IllegalArgumentException("partition cannot be null");
    }

    long now = System.currentTimeMillis();
    long deadlineTimeMs = now + timeout.toMillis();
    while (now < deadlineTimeMs) {
      LiKafkaConsumer<K, V> consumer = getPerClusterConsumerForTopic(partition.topic());
      if (consumer != null) {
        return consumer.committed(partition, timeout);
      }

      try {
        Thread.sleep(Math.min(deadlineTimeMs - System.currentTimeMillis(), _retryBackoffMs));
      } catch (InterruptedException e) {
        LOG.warn("interrupted while retrying committed()");
        throw new InterruptException(e);
      }

      now = System.currentTimeMillis();
    }
    throw new TimeoutException("Failed to complete committed() within " + timeout);
  }

  @Override
  synchronized public Long committedSafeOffset(TopicPartition partition) {
    if (partition == null) {
      throw new IllegalArgumentException("partition cannot be null");
    }

    LiKafkaConsumer<K, V> consumer = getPerClusterConsumerForTopic(partition.topic());
    if (consumer != null) {
      return consumer.committedSafeOffset(partition);
    } else {
      throw new IllegalStateException("partition " + partition + " does not exist");
    }
  }

  @Override
  synchronized public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public List<PartitionInfo> partitionsFor(String topic) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Map<String, List<PartitionInfo>> listTopics() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Set<TopicPartition> paused() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public void pause(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public void resume(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Long safeOffset(TopicPartition tp, long messageOffset) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Long safeOffset(TopicPartition tp) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Map<TopicPartition, Long> safeOffsets() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close() {
    close(CONSUMER_CLOSE_MAX_TIMEOUT);
  }

  @Override
  synchronized public void close(long timeout, TimeUnit timeUnit) {
    close(Duration.ofMillis(timeUnit.toMillis(timeout)));
  }

  // This is used for testing only
  public FederatedSubscriptionState getCurrentSubscription() {
    return _currentSubscription;
  }

  int getNumConsumersWithBootupConfigs() {
    return _numConsumersWithBootupConfigs.size();
  }

  int getNumConfigReloads() {
    return _numConfigReloads;
  }

  private void closeNoLock(Duration timeout) {
    // Get an immutable copy of the current set of consumers.
    List<ClusterConsumerPair<K, V>> consumers = _consumers;

    if (consumers.isEmpty()) {
      LOG.info("no consumers to close for cluster group {}", _clusterGroup);
      return;
    }

    LOG.debug("closing {} LiKafkaConsumers for cluster group {} in {}...", consumers.size(), _clusterGroup,
        timeout);

    long startTimeMs = System.currentTimeMillis();
    long deadlineTimeMs = startTimeMs + timeout.toMillis();
    CountDownLatch countDownLatch = new CountDownLatch(consumers.size());
    Set<Thread> threads = new HashSet<>();
    for (ClusterConsumerPair<K, V> entry : consumers) {
      ClusterDescriptor cluster = entry.getCluster();
      LiKafkaConsumer<K, V> consumer = entry.getConsumer();
      Thread t = new Thread(() -> {
        try {
          consumer.close(Duration.ofMillis(deadlineTimeMs - System.currentTimeMillis()));
        } finally {
          countDownLatch.countDown();
        }
      });
      t.setDaemon(true);
      t.setName("LiKafkaConsumer-close-" + cluster.getName());
      t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
          throw new KafkaException("Thread " + t.getName() + " throws exception", e);
        }
      });
      t.start();
      threads.add(t);
    }

    try {
      if (!countDownLatch.await(deadlineTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
        LiKafkaClientsUtils.dumpStacksForAllLiveThreads(threads);
        throw new KafkaException(
            "Fail to close all consumers for cluster group " + _clusterGroup + " in " + timeout);
      }
    } catch (InterruptedException e) {
      throw new KafkaException("Fail to close all consumers for cluster group " + _clusterGroup, e);
    }

    LOG.info("closing {} LiKafkaConsumers for cluster group {} complete in {} milliseconds", consumers.size(),
        _clusterGroup, (System.currentTimeMillis() - startTimeMs));
  }

  @Override
  synchronized public void close(Duration timeout) {
    closeNoLock(timeout);
    _closed = true;
  }

  @Override
  synchronized public void wakeup() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public LiKafkaConsumerConfig getCommonConsumerConfigs() {
    return _commonConsumerConfigs;
  }

  synchronized public void applyBootupConfigFromConductor(Map<String, String> configs) {
    _bootupConfigsFromConductor = configs;

    // Only try to recreate per-cluster consumers when _consumers are initialized, i.e. after subscribe/assign has
    // been called, otherwise it's impossible to create per-cluster consumers without the topic-cluster information,
    // just save the boot up configs
    if (_consumers != null && !_consumers.isEmpty()) {
      recreateConsumers(configs, null);
    }
  }

  public void reloadConfig(Map<String, String> newConfigs, UUID commandId) {
    // Go over each consumer, close them, and update existing configs with newConfigs. Since each per-cluster consumer will be
    // instantiated when the client began consuming from that cluster, we just need to clear the mappings and update the configs.
    // since this method is invoked by marioClient, it should be non-blocking, so we create another thread doing above
    LOG.info("Starting reloadConfig for LiKafkaConsumers in clusterGroup {} with new configs {}", _clusterGroup, newConfigs);
    Thread t = new Thread(() -> {
      recreateConsumers(newConfigs, commandId);
    });
    t.setDaemon(true);
    t.setName("LiKafkaConsumer-reloadConfig-" + _clusterGroup.getName());
    t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        LOG.error("Thread {} throws exception {}", t.getName(), e);
      }
    });

    t.start();
  }

  // Do synchronization on the method that will be closing the consumers since it will be handled by a different thread
  synchronized void recreateConsumers(Map<String, String> newConfigs, UUID commandId) {
    // TODO: ensureOpen() would throw an exception if consumers are already closed. In this case, we might want to send
    // an error message to notify mario that the config reload command fails due to consumer/producer already closed and
    // let mario decide what to do next (re-send the configs etc).
    ensureOpen();

    // Store the original per-cluster topicPartition assignment
    Map<ClusterDescriptor, Collection<TopicPartition>> perClusterTopicPartitionSet = new HashMap<>();
    for (ClusterConsumerPair<K, V> entry : _consumers) {
      perClusterTopicPartitionSet.put(entry.getCluster(), entry.getConsumer().assignment());
    }

    closeNoLock(RELOAD_CONFIG_EXECUTION_TIMEOUT);

    // save the original configs in case re-create consumers with new configs failed, we need to re-create
    // with the original configs
    Map<String, Object> originalConfigs = _commonConsumerConfigs.originals();

    // update existing configs with newConfigs
    // originals() would return a copy of the internal consumer configs, put in new configs and update existing configs
    Map<String, Object> configMap = _commonConsumerConfigs.originals();

    if (newConfigs != null && !newConfigs.isEmpty()) {
      configMap.putAll(newConfigs);
      _commonConsumerConfigs = new LiKafkaConsumerConfig(configMap);
    }

    // re-create per-cluster consumers with new set of configs
    // if any error occurs, recreate all per-cluster consumers with previous last-known-good configs and
    // TODO : send an error back to Mario
    // _consumers should be filled when reload config happens
    List<ClusterConsumerPair<K, V>> newConsumers = new ArrayList<>();
    boolean recreateConsumerSuccessful = false;

    try {
      for (ClusterConsumerPair<K, V> entry : _consumers) {
        LiKafkaConsumer<K, V> curConsumer = createPerClusterConsumer(entry.getCluster());
        newConsumers.add(new ClusterConsumerPair<K, V>(entry.getCluster(), curConsumer));
      }

      // replace _consumers with newly created consumers
      _consumers.clear();
      _consumers = newConsumers;
      recreateConsumerSuccessful = true;
    } catch (Exception e) {
      // if any exception occurs, re-create per-cluster consumers with last-known-good configs
      LOG.error("Failed to recreate per-cluster consumers with new config with exception, restore to previous consumers ", e);

      // recreate failed, restore to previous configured consumers
      newConsumers.clear();
      _commonConsumerConfigs = new LiKafkaConsumerConfig(originalConfigs);

      for (ClusterConsumerPair<K, V> entry : _consumers) {
        LiKafkaConsumer<K, V> curConsumer = createPerClusterConsumer(entry.getCluster());
        newConsumers.add(new ClusterConsumerPair<K, V>(entry.getCluster(), curConsumer));
      }

      _consumers = newConsumers;
    }

    // keep previous partition assignment after config reload
    // TODO: after subscribe() is supported, we need to also keep the original subscription
    for (Map.Entry<ClusterDescriptor, Collection<TopicPartition>> entry : perClusterTopicPartitionSet.entrySet()) {
      getOrCreatePerClusterConsumer(entry.getKey()).assign(entry.getValue());
    }

    // Convert the updated configs from Map<String, Object> to Map<String, String> and send the new config to mario server
    // report reload config execution complete to mario server
    Map<String, String> convertedConfig = new HashMap<>();
    // Send the actually used configs to Mario
    for (Map.Entry<String, Object> entry : _commonConsumerConfigs.originals().entrySet()) {
      convertedConfig.put(entry.getKey(), String.valueOf(entry.getValue()));
    }
    _mdsClient.reportCommandExecutionComplete(commandId, convertedConfig, MessageType.RELOAD_CONFIG_RESPONSE, recreateConsumerSuccessful);

    // re-register federated client with updated configs
    _mdsClient.reRegisterFederatedClient(newConfigs);

    _numConfigReloads++;

    LOG.info("Successfully recreated LiKafkaConsumers in clusterGroup {} with new configs (diff) {}", _clusterGroup, newConfigs);
  }

  // For testing only, wait for reload config command to finish since it's being executed by a different thread
  void waitForReloadConfigFinish() throws InterruptedException {
    long endWaitTime = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
    while (_numConfigReloads == 0 && System.currentTimeMillis() < endWaitTime) {
      TimeUnit.MILLISECONDS.sleep(200);
    }
  }

  private LiKafkaConsumer<K, V> getPerClusterConsumerForTopic(String topic) {
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("Topic cannot be null or empty");
    }

    ClusterDescriptor cluster = null;
    try {
      cluster = _mdsClient.getClusterForTopic(topic, _clusterGroup, _mdsRequestTimeoutMs);
    } catch (MetadataServiceClientException e) {
      throw new KafkaException("failed to get cluster for topic " + topic + ": ", e);
    }
    return (cluster == null) ? null : getPerClusterConsumer(cluster);
  }

  // Intended for testing only
  LiKafkaConsumer<K, V> getPerClusterConsumer(ClusterDescriptor cluster) {
    // Get an immutable copy of the current set of consumers.
    List<ClusterConsumerPair<K, V>> consumers = _consumers;
    for (ClusterConsumerPair<K, V> entry : consumers) {
      if (entry.getCluster().equals(cluster)) {
        return entry.getConsumer();
      }
    }
    return null;
  }

  // Returns null if the specified topic does not exist in the cluster group.
  private LiKafkaConsumer<K, V> getOrCreatePerClusterConsumer(ClusterDescriptor cluster) {
    if (cluster == null) {
      throw new IllegalArgumentException("Cluster cannot be null");
    }

    for (ClusterConsumerPair<K, V> entry : _consumers) {
      if (entry.getCluster().equals(cluster)) {
        return entry.getConsumer();
      }
    }

    LiKafkaConsumer<K, V> curConsumer = createPerClusterConsumer(cluster);
    // always update the _consumers map to avoid creating the same consumer multiple times when calling
    // this method
    _consumers.add(new ClusterConsumerPair<K, V>(cluster, curConsumer));
    return curConsumer;
  }

  private LiKafkaConsumer<K, V> createPerClusterConsumer(ClusterDescriptor cluster) {
    // Create per-cluster consumer config where the following cluster-specific properties:
    //   - bootstrap.server - the actual bootstrap URL of the physical cluster to connect to
    //   - max.poll.records - the property value set for the federated consumer / the number of clusters in this cluster group
    //                        if the federated consumer property < the number of clusters in the group, set it to 1
    //                        (poll() will make sure that it won't fetch more than the federated consumer property)
    Map<String, Object> configMap = _commonConsumerConfigs.originals();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapUrl());
    configMap.put(ConsumerConfig.CLIENT_ID_CONFIG, _clientIdPrefix + "-" + cluster.getName());

    Map<String, Object> configMapWithBootupConfig = new HashMap<>(configMap);
    // Apply the configs received from Conductor at boot up/registration time
    if (_bootupConfigsFromConductor != null && !_bootupConfigsFromConductor.isEmpty()) {
      configMapWithBootupConfig.putAll(_bootupConfigsFromConductor);
    }

    int maxPollRecordsPerCluster = Math.max(_maxPollRecordsForFederatedConsumer / _numClustersToConnectTo, 1);
    configMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecordsPerCluster);
    LiKafkaConsumer<K, V> newConsumer;

    try {
      newConsumer = _consumerBuilder.setConsumerConfig(configMapWithBootupConfig).build();
      _numConsumersWithBootupConfigs.add(newConsumer);
    } catch (Exception e) {
      LOG.error("Failed to create per-cluster consumer with config {}, try creating with config {}", configMapWithBootupConfig, configMap);
      newConsumer = _consumerBuilder.setConsumerConfig(configMap).build();
    }
    return newConsumer;
  }

  private void ensureOpen() {
    if (_closed) {
      throw new IllegalStateException("this federated consumer has already been closed");
    }
  }

  // TODO: check the client registration state and handle accordingly if it is the fallback mode
  private void refreshSubscriptionMetadata() {
    if (_closed || _currentSubscription.getSubscriptionType() == FederatedSubscriptionState.SubscriptionType.NONE ||
        _currentSubscription.getTopicsWaitingToBeCreated().isEmpty()) {
      return;
    }

    boolean topicCreated = false;
    for (String topic : _currentSubscription.getTopicsWaitingToBeCreated()) {
      try {
        if (_mdsClient.getClusterForTopic(topic, _clusterGroup, _mdsRequestTimeoutMs) != null) {
          topicCreated = true;
          break;
        }
      } catch (MetadataServiceClientException e) {
        LOG.warn("cannot get cluster info for topic {} with the following exception. ignoring...", topic, e);
      }
    }

    if (!topicCreated) {
      return;
    }

    // Re-assign/subscribe
    switch (_currentSubscription.getSubscriptionType()) {
      case USER_ASSIGNED:
        assign(((UserAssigned) _currentSubscription).getAssignment());
        break;

      case AUTO_TOPICS:
        subscribe(((Subscribed) _currentSubscription).getSubscription());
        break;

      default:
        LOG.error("unsupported subscription type: {}", _currentSubscription.getSubscriptionType().name());
        throw new IllegalStateException("Unsupportd subscription type: " + _currentSubscription.getSubscriptionType());
    }
  }

  // Callback interface for concurrently calling a method against per-cluster consumers with cluster-specific input
  // values with timeout.
  interface PerClusterExecutorCallback<K, V, I> {
    void execute(LiKafkaConsumer<K, V> consumer, I perClusterInput, Duration timeout);
  }

  // For each cluster in the perClusterInput key set, concurrently call the given callback against the corresponding
  // consumer with the input values for that cluster. If perClusterInput is null, the callback will be called for all
  // existing consumers.
  private <I> void executePerClusterCallback(String methodName,
      Map<ClusterDescriptor, I> perClusterInput, PerClusterExecutorCallback callback, Duration timeout) {
    List<ClusterConsumerPair<K, V>> consumers;
    if (perClusterInput == null) {
      // The callback needs to be executed against all consumers
      consumers = _consumers;
    } else {
      consumers = new ArrayList<>();
      for (ClusterDescriptor cluster : perClusterInput.keySet()) {
        LiKafkaConsumer<K, V> consumer = getPerClusterConsumer(cluster);
        if (cluster == null) {
          LOG.warn("consumer for cluster {} does not exist for {}. ignoring.", cluster, methodName);
        } else {
          consumers.add(new ClusterConsumerPair<K, V>(cluster, consumer));
        }
      }
    }

    if (consumers.isEmpty()) {
      // Nothing to do
      return;
    }

    LOG.debug("starting {} for {} LiKafkaConsumers for cluster group {} in {} {}", methodName, consumers.size(),
        _clusterGroup, timeout);

    long startTimeMs = System.currentTimeMillis();
    long deadlineTimeMs = startTimeMs + timeout.toMillis();
    CountDownLatch countDownLatch = new CountDownLatch(consumers.size());
    Set<Thread> threads = new HashSet<>();
    for (ClusterConsumerPair<K, V> entry : consumers) {
      ClusterDescriptor cluster = entry.getCluster();
      LiKafkaConsumer<K, V> consumer = entry.getConsumer();
      Thread t = new Thread(() -> {
        try {
          Duration remainingTime = Duration.ofMillis(Math.max(deadlineTimeMs - System.currentTimeMillis(), 0));
          callback.execute(consumer, perClusterInput == null ? null : perClusterInput.get(cluster), remainingTime);
        } finally {
          countDownLatch.countDown();
        }
      });
      t.setDaemon(true);
      t.setName("LiKafkaConsumer-" + methodName + "-" + cluster.getName());
      t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
          throw new KafkaException("Thread " + t.getName() + " throws exception", e);
        }
      });
      t.start();
      threads.add(t);
    }

    try {
      if (!countDownLatch.await(deadlineTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
        LiKafkaClientsUtils.dumpStacksForAllLiveThreads(threads);
        throw new KafkaException(
            "fail to perform " + methodName + " for cluster group " + _clusterGroup + " in " + timeout);
      }
    } catch (InterruptedException e) {
      throw new KafkaException("fail to perform " + methodName + " for cluster group " + _clusterGroup, e);
    }

    LOG.info("{}: {} LiKafkaConsumers for cluster group {} complete in {} milliseconds", methodName, consumers.size(),
        _clusterGroup, (System.currentTimeMillis() - startTimeMs));
  }

  // For the given parition to value map, construct a map ofConstruct a map of maps by cluster, where the inner map is keyed by partition.
  private <T> PartitionKeyedMapLookupResult getPartitionKeyedMapsByCluster(Map<TopicPartition, T> partitionKeyedMap) {
    if (partitionKeyedMap == null) {
      throw new IllegalArgumentException("partition map cannot be null");
    }

    if (partitionKeyedMap.isEmpty()) {
      return new PartitionKeyedMapLookupResult();
    }

    PartitionLookupResult partitionLookupResult;
    try {
      partitionLookupResult =
          _mdsClient.getClustersForTopicPartitions(partitionKeyedMap.keySet(), _clusterGroup, _mdsRequestTimeoutMs);
    } catch (MetadataServiceClientException e) {
      throw new KafkaException("failed to get clusters for topic partitions " + partitionKeyedMap.keySet() + ": ", e);
    }
    Map<ClusterDescriptor, Map<TopicPartition, T>> partitionKeyedMapsByCluster = new HashMap<>();
    for (Map.Entry<ClusterDescriptor, Set<TopicPartition>> entry : partitionLookupResult.getPartitionsByCluster().entrySet()) {
      ClusterDescriptor cluster = entry.getKey();
      Set<TopicPartition> topicPartitions = entry.getValue();
      for (TopicPartition topicPartition : entry.getValue()) {
        partitionKeyedMapsByCluster.computeIfAbsent(cluster, k -> new HashMap<TopicPartition, T>())
            .put(topicPartition, partitionKeyedMap.get(topicPartition));
      }
    }

    return new PartitionKeyedMapLookupResult(partitionKeyedMapsByCluster, partitionLookupResult.getNonexistentTopics());
  }
}
