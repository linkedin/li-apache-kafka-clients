/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.common.MetricsProxy;
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
 * @param <K>
 * @param <V>
 */
public class LiKafkaInstrumentedConsumerImpl<K, V> implements DelegatingConsumer<K, V>, EventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaInstrumentedConsumerImpl.class);

  private final long initialConnectionTimeoutMs = TimeUnit.SECONDS.toMillis(30);
  private final ReadWriteLock delegateLock = new ReentrantReadWriteLock();
  private final Properties baseConfig;
  private final ConsumerFactory<K, V> consumerFactory;
  private final Map<String, String> libraryVersions;
  private final CountDownLatch initialConnectionLatch = new CountDownLatch(1);
  private final MetricsProxy metricsProxy = new MetricsProxy() {
    @Override
    protected Map<MetricName, ? extends Metric> getMetrics() {
      Consumer<K, V> delegate = LiKafkaInstrumentedConsumerImpl.this.delegate;
      return delegate == null ? Collections.emptyMap() : delegate.metrics();
    }
  };
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
    List<String> conversionIssues = new ArrayList<>(1);
    this.baseConfig = baseConfig;
    Map<String, String> translatedBaseConfig = LiKafkaClientsUtils.propertiesToStringMap(baseConfig, conversionIssues);
    this.consumerFactory = consumerFactory;
    this.libraryVersions = LiKafkaClientsUtils.getKnownLibraryVersions();
    if (libraryVersions != null && !libraryVersions.isEmpty()) {
      this.libraryVersions.putAll(libraryVersions); //user input overrides built-ins
    }

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
          LOG.error("unable to contact {} within timeout ({}), using user-provided configs as fallback",
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
    delegateLock.writeLock().lock();
    try {
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
    } finally {
      delegateLock.writeLock().unlock();
    }
  }

  @Override
  public Set<TopicPartition> assignment() {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.assignment();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Set<String> subscription() {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.subscription();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void subscribe(Collection<String> topics) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      subscribedTopics = topics;
      rebalanceListener = null;
      assignedPartitions = null;
      subscriptionPattern = null;

      delegate.subscribe(topics);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      subscribedTopics = topics;
      rebalanceListener = callback;
      assignedPartitions = null;
      subscriptionPattern = null;

      delegate.subscribe(topics, callback);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      subscribedTopics = null;
      rebalanceListener = null;
      assignedPartitions = partitions;
      subscriptionPattern = null;

      delegate.assign(partitions);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      subscribedTopics = null;
      rebalanceListener = callback;
      assignedPartitions = null;
      subscriptionPattern = pattern;

      delegate.subscribe(pattern, callback);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void subscribe(Pattern pattern) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      subscribedTopics = null;
      rebalanceListener = null;
      assignedPartitions = null;
      subscriptionPattern = pattern;

      delegate.subscribe(pattern);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void unsubscribe() {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      subscribedTopics = null;
      rebalanceListener = null;
      assignedPartitions = null;
      subscriptionPattern = null;

      delegate.unsubscribe();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  @Deprecated
  public ConsumerRecords<K, V> poll(long timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.poll(timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public ConsumerRecords<K, V> poll(Duration timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.poll(timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void commitSync() {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.commitSync();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void commitSync(Duration timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.commitSync(timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.commitSync(offsets);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.commitSync(offsets, timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void commitAsync() {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.commitAsync();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.commitAsync(callback);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.commitAsync(offsets, callback);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.seek(partition, offset);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.seekToBeginning(partitions);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.seekToEnd(partitions);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public long position(TopicPartition partition) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.position(partition);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public long position(TopicPartition partition, Duration timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.position(partition, timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.committed(partition);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.committed(partition, timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    //noinspection unchecked
    return (Map<MetricName, ? extends Metric>) metricsProxy;
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.partitionsFor(topic);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.partitionsFor(topic, timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.listTopics();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.listTopics(timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Set<TopicPartition> paused() {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.paused();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.pause(partitions);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.resume(partitions);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.offsetsForTimes(timestampsToSearch);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
      Duration timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.offsetsForTimes(timestampsToSearch, timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.beginningOffsets(partitions);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.beginningOffsets(partitions, timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.endOffsets(partitions);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.endOffsets(partitions, timeout);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void close() {
    if (!proceedClosing()) {
      return;
    }
    try {
      delegate.close();
    } finally {
      delegate = null;
      closeMdsClient();
    }
  }

  @Override
  @Deprecated
  public void close(long timeout, TimeUnit unit) {
    if (!proceedClosing()) {
      return;
    }
    try {
      delegate.close(timeout, unit);
    } finally {
      delegate = null;
      closeMdsClient();
    }
  }

  @Override
  public void close(Duration timeout) {
    if (!proceedClosing()) {
      return;
    }
    try {
      delegate.close(timeout);
    } finally {
      delegate = null;
      closeMdsClient();
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

  private boolean proceedClosing() {
    if (isClosed()) {
      return false;
    }
    delegateLock.writeLock().lock();
    try {
      if (isClosed()) {
        return false;
      }
      closedAt = System.currentTimeMillis();
      return true;
    } finally {
      delegateLock.writeLock().unlock();
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
    if (isClosed()) {
      //same exception thats thrown by a vanilla consumer
      throw new IllegalStateException("This consumer has already been closed.");
    }
  }
}
