/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.common.LiKafkaFederatedClient;
import com.linkedin.kafka.clients.common.LiKafkaFederatedClientType;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClient;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClientException;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;

import com.linkedin.mario.common.websockets.MsgType;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a producer implementation that works with a federated Kafka cluster, which consists of one or more physical
 * Kafka clusters.
 */
public class LiKafkaFederatedProducerImpl<K, V> implements LiKafkaProducer<K, V>, LiKafkaFederatedClient {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaFederatedProducerImpl.class);
  private static final AtomicInteger FEDERATED_PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
  private static final Duration RELOAD_CONFIG_EXECUTION_TIME_OUT = Duration.ofMinutes(10);

  // The cluster group this client is talking to
  private ClusterGroupDescriptor _clusterGroup;

  // The client for the metadata service which serves cluster and topic metadata
  private MetadataServiceClient _mdsClient;

  // Timeout in milliseconds for metadata service requests.
  private int _mdsRequestTimeoutMs;

  // Per cluster producers
  private Map<ClusterDescriptor, LiKafkaProducer<K, V>> _producers;

  // Producer builder for creating per-cluster LiKafkaProducer
  private LiKafkaProducerBuilder<K, V> _producerBuilder;

  // Producer configs common to all clusters
  private LiKafkaProducerConfig _commonProducerConfigs;

  // The prefix of the client.id property to be used for individual producers. Since the client id is part of the
  // producer metric keys, we need to use cluster-specific client ids to differentiate metrics from different clusters.
  // Per-cluster client id is generated by appending the cluster name to this prefix.
  //
  // If user set the client.id property for the federated client, it will be used as the prefix. If not, a new prefix
  // is generated.
  private String _clientIdPrefix;

  private volatile boolean _closed;

  // Number of config reload operations executed
  private volatile int _numConfigReloads;

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
    _clusterGroup = new ClusterGroupDescriptor(configs.getString(LiKafkaProducerConfig.CLUSTER_GROUP_CONFIG),
        configs.getString(LiKafkaProducerConfig.CLUSTER_ENVIRONMENT_CONFIG));

    // Each per-cluster producer and auditor will be intantiated by the passed-in producer builder when the client
    // begins to produce to that cluster. If a null builder is passed, create a default one, which builds LiKafkaProducer.
    _producers = new ConcurrentHashMap<ClusterDescriptor, LiKafkaProducer<K, V>>();
    _producerBuilder = producerBuilder != null ? producerBuilder : new LiKafkaProducerBuilder<K, V>();

    _mdsRequestTimeoutMs = configs.getInt(LiKafkaProducerConfig.METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG);

    String clientIdPrefix = (String) configs.originals().get(ProducerConfig.CLIENT_ID_CONFIG);
    if (clientIdPrefix == null || clientIdPrefix.length() == 0) {
      clientIdPrefix = "producer-" + FEDERATED_PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
    }
    _clientIdPrefix = clientIdPrefix;

    _closed = false;
    _numConfigReloads = 0;

    try {
      // Instantiate metadata service client if necessary.
      _mdsClient = mdsClient != null ? mdsClient :
          configs.getConfiguredInstance(LiKafkaProducerConfig.METADATA_SERVICE_CLIENT_CLASS_CONFIG, MetadataServiceClient.class);

      // Register this federated client with the metadata service. The metadata service will assign a UUID to this
      // client, which will be used for later interaction between the metadata service and the client.
      //
      // Registration may also return further information such as the metadata server version and any protocol settings.
      // We assume that such information will be kept and used by the metadata service client itself.
      //
      // TODO: make sure this is not blocking indefinitely and also works when Mario is not available.
      _mdsClient.registerFederatedClient(this, _clusterGroup, configs.originals(), _mdsRequestTimeoutMs);
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
  public LiKafkaFederatedClientType getClientType() {
    return LiKafkaFederatedClientType.FEDERATED_PRODUCER;
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
    return send(producerRecord, null);
  }

  @Override
  synchronized public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
    return getOrCreateProducerForTopic(producerRecord.topic()).send(producerRecord, callback);
  }

  @Override
  public void flush() {
    flush(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  synchronized public void flush(long timeout, TimeUnit timeUnit) {
    flushNoLock(timeout, timeUnit);
  }

  void flushNoLock(long timeout, TimeUnit timeUnit) {
    if (_producers.isEmpty()) {
      LOG.info("no producers to flush for cluster group {}", _clusterGroup);
      return;
    }

    ensureOpen();

    LOG.info("flushing LiKafkaProducer for cluster group {} in {} {}...", _clusterGroup, timeout, timeUnit);

    long startTimeMs = System.currentTimeMillis();
    long deadlineTimeMs = startTimeMs + timeUnit.toMillis(timeout);
    CountDownLatch countDownLatch = new CountDownLatch(_producers.entrySet().size());
    Set<Thread> threads = new HashSet<>();
    for (Map.Entry<ClusterDescriptor, LiKafkaProducer<K, V>> entry : _producers.entrySet()) {
      ClusterDescriptor cluster = entry.getKey();
      LiKafkaProducer<K, V> producer = entry.getValue();
      Thread t = new Thread(() -> {
        try {
          producer.flush(deadlineTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        } finally {
          countDownLatch.countDown();
        }
      });
      t.setDaemon(true);
      t.setName("LiKafkaProducer-flush-" + cluster.getName());
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
        throw new KafkaException("Fail to flush all producers for cluster group " + _clusterGroup + " in " +
            timeout + " " + timeUnit);
      }
    } catch (InterruptedException e) {
      throw new KafkaException("Fail to flush all producers for cluster group " + _clusterGroup, e);
    }

    LOG.info("LiKafkaProducer flush for cluster group {} complete in {} milliseconds", _clusterGroup,
        (System.currentTimeMillis() - startTimeMs));
  }

  @Override
  synchronized public List<PartitionInfo> partitionsFor(String topic) {
    return getOrCreateProducerForTopic(topic).partitionsFor(topic);
  }

  @Override
  synchronized public Map<String, List<PartitionInfo>> partitionsFor(Set<String> topics) {
    // TODO: come back here when upstream API settles
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  synchronized public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close() {
    close(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  synchronized public void close(long timeout, TimeUnit timeUnit) {
    closeNoLock(timeout, timeUnit);
  }

  void closeNoLock(long timeout, TimeUnit timeUnit) {
    if (_producers.isEmpty()) {
      LOG.debug("no producers to close for cluster group {}", _clusterGroup);
      return;
    }

    if (_closed) {
      return;
    }

    _closed = true;

    LOG.info("closing LiKafkaProducer for cluster group {} in {} {}...", _clusterGroup, timeout, timeUnit);

    long startTimeMs = System.currentTimeMillis();
    long deadlineTimeMs = startTimeMs + timeUnit.toMillis(timeout);
    CountDownLatch countDownLatch = new CountDownLatch(_producers.entrySet().size());
    Set<Thread> threads = new HashSet<>();
    for (Map.Entry<ClusterDescriptor, LiKafkaProducer<K, V>> entry : _producers.entrySet()) {
      ClusterDescriptor cluster = entry.getKey();
      LiKafkaProducer<K, V> producer = entry.getValue();
      Thread t = new Thread(() -> {
        try {
          producer.close(deadlineTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        } finally {
          countDownLatch.countDown();
        }
      });
      t.setDaemon(true);
      t.setName("LiKafkaProducer-close-" + cluster.getName());
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
        throw new KafkaException("Fail to close all producers for cluster group " + _clusterGroup + " in " +
            timeout + " " + timeUnit);
      }
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

  public LiKafkaProducerConfig getCommonProducerConfigs() {
    return _commonProducerConfigs;
  }

  public void reloadConfig(Map<String, String> newConfigs, UUID commandId) {
    // Go over each producer, flush and close. Since each per-cluster producer will be instantiated when the client began
    // producing to that cluster, we just need to clear the mappings and update the configs
    // since this method is invoked by marioClient, it should be non-blocking, so we create another thread doing above
    LOG.info("Starting reloadConfig for LiKafkaProducers in clusterGroup {} with new configs {}", _clusterGroup, newConfigs);
    Thread t = new Thread(() -> {
        recreateProducers(newConfigs, commandId);
    });
    t.setDaemon(true);
    t.setName("LiKafkaProducer-reloadConfig-" + _clusterGroup.getName());
    t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        LOG.error("Thread {} throws exception {}", t.getName(), e);
      }
    });

    t.start();
  }

  // Do synchronization on the method that will be flushing and closing the producers since it will be handled by a different thread
  synchronized void recreateProducers(Map<String, String> newConfigs, UUID commandId) {
    ensureOpen();

    flushNoLock(RELOAD_CONFIG_EXECUTION_TIME_OUT.toMillis(), TimeUnit.MILLISECONDS);
    closeNoLock(RELOAD_CONFIG_EXECUTION_TIME_OUT.toMillis(), TimeUnit.MILLISECONDS);

    _closed = false;

    // save the original configs in case re-create producers with new configs failed, we need to re-create
    // with the original configs
    Map<String, Object> originalConfig = _commonProducerConfigs.originals();

    // update existing configs with newConfigs
    // originals() would return a copy of the internal producer configs, put in new configs and update existing configs
    Map<String, Object> configMap = _commonProducerConfigs.originals();
    configMap.putAll(newConfigs);
    _commonProducerConfigs = new LiKafkaProducerConfig(configMap);

    // re-create per-cluster producers with new set of configs
    // if any error occurs, recreate all per-cluster producers with previous last-known-good configs and
    // TODO : send an error back to Mario
    // _producers should be filled when reload config happens
    Map<ClusterDescriptor, LiKafkaProducer<K, V>> newProducers = new ConcurrentHashMap<>();

    try {
      for (Map.Entry<ClusterDescriptor, LiKafkaProducer<K, V>> entry : _producers.entrySet()) {
        LiKafkaProducer<K, V> curProducer = createPerClusterProducer(entry.getKey());
        newProducers.put(entry.getKey(), curProducer);
      }

      // replace _producers with newly created producers
      _producers.clear();
      _producers = newProducers;
    } catch (Exception e) {
      LOG.error("Failed to recreate per-cluster producers with new configs with exception, restore to previous producers ", e);

      // recreate failed, restore to previous configured producers
      newProducers.clear();
      _commonProducerConfigs = new LiKafkaProducerConfig(originalConfig);

      for (Map.Entry<ClusterDescriptor, LiKafkaProducer<K, V>> entry : _producers.entrySet()) {
        LiKafkaProducer<K, V> curProducer = createPerClusterProducer(entry.getKey());
        newProducers.put(entry.getKey(), curProducer);
      }

      _producers = newProducers;
    }

    // Convert the updated configs from Map<String, Object> to Map<String, String> and send the new config to mario server
    // report reload config execution complete to mario server
    Map<String, String> convertedConfig = new HashMap<>();
    for (Map.Entry<String, Object> entry : _commonProducerConfigs.originals().entrySet()) {
      convertedConfig.put(entry.getKey(), String.valueOf(entry.getValue()));
    }
    // TODO: add a flag in RELOAD_CONFIG_RESPONSE message to indicate result of config reload
    _mdsClient.reportCommandExecutionComplete(commandId, convertedConfig, MsgType.RELOAD_CONFIG_RESPONSE);

    // re-register federated client with updated configs
    _mdsClient.reRegisterFederatedClient(newConfigs);

    _numConfigReloads++;

    LOG.info("Successfully recreated LiKafkaProducers in clusterGroup {} with new configs (diff) {}", _clusterGroup, newConfigs);
  }

  // For testing only, wait for reload config command to finish since it's being executed by a different thread
  void waitForReloadConfigFinish() throws InterruptedException {
    long endWaitTime = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
    while (_numConfigReloads == 0 && System.currentTimeMillis() < endWaitTime) {
      TimeUnit.MILLISECONDS.sleep(200);
    }
  }

  // Intended for testing only
  LiKafkaProducer<K, V> getPerClusterProducer(ClusterDescriptor cluster) {
    return _producers.get(cluster);
  }

  private LiKafkaProducer<K, V> getOrCreateProducerForTopic(String topic) {
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("Topic cannot be null or empty");
    }

    ensureOpen();

    // TODO: Handle nonexistent topics more elegantly with auto topic creation option
    ClusterDescriptor cluster = null;
    try {
      cluster = _mdsClient.getClusterForTopic(topic, _clusterGroup, _mdsRequestTimeoutMs);
    } catch (MetadataServiceClientException e) {
      throw new KafkaException("failed to get cluster for topic " + topic + ": ", e);
    }
    if (cluster == null) {
      throw new IllegalStateException("Topic " + topic + " not found in the metadata service");
    }
    return getOrCreatePerClusterProducer(cluster);
  }

  private LiKafkaProducer<K, V> getOrCreatePerClusterProducer(ClusterDescriptor cluster) {
    if (cluster == null) {
      throw new IllegalArgumentException("Cluster cannot be null");
    }

    ensureOpen();

    if (_producers.containsKey(cluster)) {
      return _producers.get(cluster);
    }

    LiKafkaProducer<K, V> newProducer = createPerClusterProducer(cluster);
    // always update _producers map to avoid creating multiple producers when calling this method
    // multiple times
    _producers.put(cluster, newProducer);
    return newProducer;
  }

  private LiKafkaProducer<K, V> createPerClusterProducer(ClusterDescriptor cluster) {
    if (cluster == null) {
      throw new IllegalArgumentException("Cluster cannot be null");
    }

    ensureOpen();

    // Create per-cluster producer config
    Map<String, Object> configMap = _commonProducerConfigs.originals();
    configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapUrl());
    configMap.put(ProducerConfig.CLIENT_ID_CONFIG, _clientIdPrefix + "-" + cluster.getName());

    _producerBuilder.setProducerConfig(configMap);
    LiKafkaProducer<K, V> newProducer = _producerBuilder.build();
    return newProducer;
  }

  private void ensureOpen() {
    if (_closed) {
      throw new IllegalStateException("this federated producer has already been closed");
    }
  }
}
