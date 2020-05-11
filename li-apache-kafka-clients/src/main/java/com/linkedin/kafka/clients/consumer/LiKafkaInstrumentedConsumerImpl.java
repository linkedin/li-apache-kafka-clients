/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.common.MetricsProxy;
import com.linkedin.kafka.clients.utils.CloseableLock;
import com.linkedin.kafka.clients.utils.KafkaConsumerLock;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.mario.client.EventHandler;
import com.linkedin.mario.client.SimpleClient;
import com.linkedin.mario.client.SimpleClientState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
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
 * an instrumented consumer is a decorator around a single delegate client
 * that supports registration with conductor, telemetry, and remote control
 * (for example, pushing config changes from conductor side)
 * @param <K> key type
 * @param <V> value type
 */
public class LiKafkaInstrumentedConsumerImpl<K, V> implements DelegatingConsumer<K, V>, EventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaInstrumentedConsumerImpl.class);

  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  private final long initialConnectionTimeoutMs;
  private final ReadWriteLock delegateLock = new ReentrantReadWriteLock();
  private final Properties baseConfig;
  private final ConsumerFactory<K, V> consumerFactory;
  @SuppressWarnings("FieldCanBeLocal")
  private final Map<String, String> libraryVersions;
  private final CountDownLatch initialConnectionLatch = new CountDownLatch(1);
  private final MetricsProxy metricsProxy = new MetricsProxy() {
    @Override
    protected Map<MetricName, ? extends Metric> getMetrics() {
      Consumer<K, V> delegate = LiKafkaInstrumentedConsumerImpl.this.delegate;
      return delegate == null ? Collections.emptyMap() : delegate.metrics();
    }
  };

  //just like vanilla kafka clients, these keep track of the user thread currently operating the client
  private final KafkaConsumerLock userLock = new KafkaConsumerLock();

  //records when the client was closed and by whom
  private volatile boolean closing = false;
  private volatile Exception closer = null;
  private volatile Long closedAt = null;

  private volatile Map<String, String> configOverrides = null;
  private volatile Consumer<K, V> delegate;
  private final SimpleClient mdsClient;

  private volatile Collection<String> subscribedTopics = null;
  private volatile ConsumerRebalanceListener rebalanceListener = null;
  private volatile Collection<TopicPartition> assignedPartitions = null;
  private volatile Pattern subscriptionPattern = null;

  @Deprecated
  public LiKafkaInstrumentedConsumerImpl(
      Properties baseConfig,
      ConsumerFactory<K, V> consumerFactory,
      Supplier<String> mdsUrlSupplier
  ) {
    this(baseConfig, null, consumerFactory, mdsUrlSupplier);
  }

  public LiKafkaInstrumentedConsumerImpl(
      Properties baseConfig,
      Map<String, String> libraryVersions,
      ConsumerFactory<K, V> consumerFactory,
      Supplier<String> mdsUrlSupplier
  ) {
    this(baseConfig, libraryVersions, consumerFactory, mdsUrlSupplier, TimeUnit.SECONDS.toMillis(10));
  }

  public LiKafkaInstrumentedConsumerImpl(
      Properties baseConfig,
      Map<String, String> libraryVersions,
      ConsumerFactory<K, V> consumerFactory,
      Supplier<String> mdsUrlSupplier,
      long initialConnectionTimeoutMs
  ) {
    List<String> conversionIssues = new ArrayList<>(1);
    this.baseConfig = baseConfig;
    Map<String, String> translatedBaseConfig = LiKafkaClientsUtils.propertiesToStringMap(baseConfig, conversionIssues);
    this.consumerFactory = consumerFactory;
    this.libraryVersions = LiKafkaClientsUtils.getKnownLibraryVersions();
    if (libraryVersions != null && !libraryVersions.isEmpty()) {
      this.libraryVersions.putAll(libraryVersions); //user input overrides built-ins
    }
    this.initialConnectionTimeoutMs = initialConnectionTimeoutMs;

    if (!conversionIssues.isEmpty()) {
      StringJoiner csv = new StringJoiner(", ");
      conversionIssues.forEach(csv::add);
      LOG.error("issues translating consumer config to strings: {}", csv);
    }

    mdsClient = new SimpleClient(mdsUrlSupplier,
        TimeUnit.MINUTES.toMillis(1),
        TimeUnit.HOURS.toMillis(1),
        translatedBaseConfig,
        this.libraryVersions,
        this
    );

    boolean tryFallback;
    Exception issue = null;
    try {
      tryFallback = !initialConnectionLatch.await(initialConnectionTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      tryFallback = true;
      issue = e;
    }

    if (tryFallback) {
      boolean delegateChanged = recreateDelegate(true);
      if (delegateChanged) {
        if (issue != null) {
          LOG.error("exception waiting to contact {}, using user-provided configs as fallback",
              mdsClient.getLastAttemptedMarioUrl(), issue);
        } else {
          LOG.warn("unable to contact {} within timeout ({}), using user-provided configs as fallback",
              mdsClient.getLastAttemptedMarioUrl(), initialConnectionTimeoutMs);
        }
      } else if (issue != null) {
        //we got interrupted waiting, but apparently connection to mds was successful?
        LOG.warn("exception waiting on MDS connection, yet connection succeeded?", issue);
      }
    }
  }

  @Override
  public void stateChanged(SimpleClient client, SimpleClientState oldState, SimpleClientState newState) {
    if (newState == SimpleClientState.REGISTERED) {
      try {
        Map<String, String> newOverrides = client.getConfigOverrides();
        Map<String, String> currentOverrides = this.configOverrides;
        boolean configOverrideChanged = !Objects.equals(currentOverrides, newOverrides);
        if (delegate == null || configOverrideChanged) {
          if (configOverrideChanged) {
            LOG.info("got new config overrides from {}: {}", mdsClient.getLastConnectedMarioUrl(), newOverrides);
          } else {
            LOG.info("successfully connected to {}, no config overrides", mdsClient.getLastConnectedMarioUrl());
          }
          this.configOverrides = newOverrides;
          recreateDelegate(false);
        }
      } finally {
        initialConnectionLatch.countDown();
      }
    }
  }

  @Override
  public void configChangeRequested(UUID commandId, Map<String, String> configDiff, String message) {
    Map<String, String> currentOverrides = this.configOverrides;
    if (!Objects.equals(currentOverrides, configDiff)) {
      LOG.info("got new config overrides from {}: {} ({})", mdsClient.getLastConnectedMarioUrl(), configDiff, message);
      this.configOverrides = configDiff;
      recreateDelegate(false);
    }
    //TODO - respond to command UUID
  }

  @Override
  public Consumer<K, V> getDelegate() {
    return delegate;
  }

  /**
   * close any existing delegate client and create a new one based on the
   * latest config overrides
   * @param abortIfExists abort the operation if current delegate is not null
   * @return true if delegate client actually replaced
   */
  private boolean recreateDelegate(boolean abortIfExists) {
    try (@SuppressWarnings("unused") CloseableLock swLock = new CloseableLock(delegateLock.writeLock())) {
      Set<TopicPartition> pausedPartitions = null;
      Consumer<K, V> prevConsumer = delegate;
      if (prevConsumer != null) {
        if (abortIfExists) {
          return false; //leave existing delegate as-is
        }
        pausedPartitions = prevConsumer.paused();
        delegate = null;
        try {
          try {
            prevConsumer.commitSync(Duration.ofSeconds(30));
          } finally {
            prevConsumer.close(Duration.ofSeconds(10));
          }
        } catch (Exception e) {
          LOG.error("error closing old delegate consumer", e);
        }
      }

      if (closing) {
        return false;
      }

      delegate = consumerFactory.create(baseConfig, LiKafkaClientsUtils.convertConfigMapToProperties(configOverrides));

      if (subscriptionPattern != null) {
        if (rebalanceListener != null) {
          delegate.subscribe(subscriptionPattern, rebalanceListener);
        } else {
          delegate.subscribe(subscriptionPattern);
        }
      } else if (subscribedTopics != null) {
        if (rebalanceListener != null) {
          delegate.subscribe(subscribedTopics, rebalanceListener);
        } else {
          delegate.subscribe(subscribedTopics);
        }
      } else if (assignedPartitions != null) {
        delegate.assign(assignedPartitions);
      }
      if (pausedPartitions != null && !pausedPartitions.isEmpty()) {
        //TODO - this may throw exception if rebalance hasnt completed. test this
        delegate.pause(pausedPartitions);
      }
      return true;
    }
  }

  @Override
  public Set<TopicPartition> assignment() {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.assignment();
    }
  }

  @Override
  public Set<String> subscription() {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      //record
      return delegate.subscription();
    }
  }

  @Override
  public void subscribe(Collection<String> topics) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      subscribedTopics = topics;
      rebalanceListener = null;
      assignedPartitions = null;
      subscriptionPattern = null;

      delegate.subscribe(topics);
    }
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      subscribedTopics = topics;
      rebalanceListener = callback;
      assignedPartitions = null;
      subscriptionPattern = null;

      delegate.subscribe(topics, callback);
    }
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      subscribedTopics = null;
      rebalanceListener = null;
      assignedPartitions = partitions;
      subscriptionPattern = null;

      delegate.assign(partitions);
    }
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      subscribedTopics = null;
      rebalanceListener = callback;
      assignedPartitions = null;
      subscriptionPattern = pattern;

      delegate.subscribe(pattern, callback);
    }
  }

  @Override
  public void subscribe(Pattern pattern) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      subscribedTopics = null;
      rebalanceListener = null;
      assignedPartitions = null;
      subscriptionPattern = pattern;

      delegate.subscribe(pattern);
    }
  }

  @Override
  public void unsubscribe() {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      subscribedTopics = null;
      rebalanceListener = null;
      assignedPartitions = null;
      subscriptionPattern = null;

      delegate.unsubscribe();
    }
  }

  @Override
  @Deprecated
  public ConsumerRecords<K, V> poll(long timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.poll(timeout);
    }
  }

  @Override
  public ConsumerRecords<K, V> poll(Duration timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.poll(timeout);
    }
  }

  @Override
  public void commitSync() {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.commitSync();
    }
  }

  @Override
  public void commitSync(Duration timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.commitSync(timeout);
    }
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.commitSync(offsets);
    }
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.commitSync(offsets, timeout);
    }
  }

  @Override
  public void commitAsync() {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.commitAsync();
    }
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.commitAsync(callback);
    }
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.commitAsync(offsets, callback);
    }
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.seek(partition, offset);
    }
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.seekToBeginning(partitions);
    }
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.seekToEnd(partitions);
    }
  }

  @Override
  public long position(TopicPartition partition) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.position(partition);
    }
  }

  @Override
  public long position(TopicPartition partition, Duration timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.position(partition, timeout);
    }
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.committed(partition);
    }
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.committed(partition, timeout);
    }
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    //noinspection unchecked
    return (Map<MetricName, ? extends Metric>) metricsProxy;
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.partitionsFor(topic);
    }
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.partitionsFor(topic, timeout);
    }
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.listTopics();
    }
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.listTopics(timeout);
    }
  }

  @Override
  public Set<TopicPartition> paused() {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.paused();
    }
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.pause(partitions);
    }
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      delegate.resume(partitions);
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.offsetsForTimes(timestampsToSearch);
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
      Duration timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.offsetsForTimes(timestampsToSearch, timeout);
    }
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.beginningOffsets(partitions);
    }
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.beginningOffsets(partitions, timeout);
    }
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.endOffsets(partitions);
    }
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    try (
        @SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock);
        @SuppressWarnings("unused") CloseableLock srLock = new CloseableLock(delegateLock.readLock())
    ) {
      verifyOpen();
      return delegate.endOffsets(partitions, timeout);
    }
  }

  @Override
  public void close() {
    try (@SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock)) {
      if (!proceedClosing()) {
        return;
      }
      try {
        Consumer<K, V> delegate = this.delegate;
        if (delegate != null) {
          delegate.close();
        }
      } finally {
        this.delegate = null;
        closeMdsClient();
      }
    }
  }

  @Override
  @Deprecated
  public void close(long timeout, TimeUnit unit) {
    try (@SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock)) {
      if (!proceedClosing()) {
        return;
      }
      try {
        Consumer<K, V> delegate = this.delegate;
        if (delegate != null) {
          delegate.close(timeout, unit);
        }
      } finally {
        this.delegate = null;
        closeMdsClient();
      }
    }
  }

  @Override
  public void close(Duration timeout) {
    try (@SuppressWarnings("unused") CloseableLock uLock = new CloseableLock(userLock)) {
      if (!proceedClosing()) {
        return;
      }
      try {
        Consumer<K, V> delegate = this.delegate;
        if (delegate != null) {
          delegate.close(timeout);
        }
      } finally {
        this.delegate = null;
        closeMdsClient();
      }
    }
  }

  @Override
  public void wakeup() {
    //vanilla allows this even if closed
    Consumer<K, V> delegate = this.delegate;
    if (delegate != null) {
      delegate.wakeup();
    }
  }

  /**
   * ensures that close only happens once, also records who closed the client and when
   * @return true if current call is the 1st close call and should proceed
   */
  private boolean proceedClosing() {
    if (isClosed()) {
      return false;
    }
    closing = true;
    try (@SuppressWarnings("unused") CloseableLock swLock = new CloseableLock(delegateLock.writeLock())) {
      if (isClosed()) {
        return false;
      }
      closer = new Exception(); //capture current stack
      closedAt = System.currentTimeMillis();
      return true;
    }
  }

  private void closeMdsClient() {
    try {
      mdsClient.close();
    } catch (Exception e) {
      LOG.warn("error closing conductor client", e);
    }
  }

  private boolean isClosed() {
    return closedAt != null;
  }

  /**
   * throws an exception if this consumer has been closed.
   * there's also an assumption that unless closed, delegate is always != null
   */
  private void verifyOpen() {
    if (closing) {
      //same exception thats thrown by a vanilla consumer
      throw new IllegalStateException("This consumer has already been closed. see following exception for point of close", closer);
    }
  }
}
