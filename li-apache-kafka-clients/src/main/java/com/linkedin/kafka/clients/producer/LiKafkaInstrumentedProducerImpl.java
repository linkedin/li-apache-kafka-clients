/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.common.MetricsProxy;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.mario.client.EventHandler;
import com.linkedin.mario.client.SimpleClient;
import com.linkedin.mario.client.SimpleClientState;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * an instrumented producer is a decorator around a single delegate client
 * that supports registration with conductor, telemetry, and remote control
 * (for example, pushing config changes from conductor side)
 * @param <K>
 * @param <V>
 */
public class LiKafkaInstrumentedProducerImpl<K, V> implements Producer<K, V>, EventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaInstrumentedProducerImpl.class);
  private static final String BOUNDED_FLUSH_THREAD_PREFIX = "Bounded-Flush-Thread-";

  private final long initialConnectionTimeoutMs = TimeUnit.SECONDS.toMillis(30);
  private final ReadWriteLock delegateLock = new ReentrantReadWriteLock();
  private final Properties baseConfig;
  private final Function<Properties, Producer<K, V>> producerFactory;
  private final String mdsUrl;
  private final CountDownLatch initialConnectionLatch = new CountDownLatch(1);
  private final MetricsProxy metricsProxy = new MetricsProxy() {
    @Override
    protected Map<MetricName, ? extends Metric> getMetrics() {
      Producer<K, V> delegate = LiKafkaInstrumentedProducerImpl.this.delegate;
      return delegate == null ? Collections.emptyMap() : delegate.metrics();
    }
  };
  private volatile Long closedAt = null;
  private volatile Map<String, String> configOverrides;
  private volatile Producer<K, V> delegate;
  private final SimpleClient mdsClient;

  // This is null if the underlying producer does not have an implementation for time-bounded flush
  private Method boundedFlushMethod;
  private final AtomicInteger boundFlushThreadCount = new AtomicInteger();

  public LiKafkaInstrumentedProducerImpl(
      Properties baseConfig,
      Function<Properties, Producer<K, V>> producerFactory,
      String mdsUrl
  ) {
    List<String> conversionIssues = new ArrayList<>(1);
    this.baseConfig = baseConfig;
    Map<String, String> translatedBaseConfig = LiKafkaClientsUtils.propertiesToStringMap(baseConfig, conversionIssues);
    this.producerFactory = producerFactory;
    this.mdsUrl = mdsUrl;

    if (!conversionIssues.isEmpty()) {
      StringJoiner csv = new StringJoiner(", ");
      conversionIssues.forEach(csv::add);
      LOG.error("issues translating producer config to strings: {}", csv);
    }

    mdsClient = new SimpleClient(
        mdsUrl,
        TimeUnit.MINUTES.toMillis(1),
        TimeUnit.HOURS.toMillis(1), translatedBaseConfig,
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
          LOG.error("exception waiting to contact {}, using user-provided configs as fallback", mdsUrl, issue);
        } else {
          LOG.error("unable to contact {} within timeout ({}), using user-provided configs as fallback", mdsUrl, initialConnectionTimeoutMs);
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
            LOG.info("got new config overrides from {}: {}", mdsUrl, newOverrides);
          } else {
            LOG.info("successfully connected to {}, no config overrides", mdsUrl);
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
      LOG.info("got new config overrides from {}: {} ({})", mdsUrl, configDiff, message);
      this.configOverrides = configDiff;
      recreateDelegate(false);
    }
    //TODO - respond to command UUID
  }

  //package-private FOR TESTING
  Producer<K, V> getDelegate() {
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
      Producer<K, V> prevProducer = delegate;
      if (prevProducer != null) {
        if (abortIfExists) {
          return false; //leave existing delegate as-is
        }
        delegate = null;
        try {
          try {
            prevProducer.flush();
          } finally {
            //TODO - fix back after bumping up kafka-clients
            prevProducer.close(10, TimeUnit.SECONDS);
          }
        } catch (Exception e) {
          LOG.error("error closing old delegate producer", e);
        }
        //TODO - keep track of ongoing transactions, and cancel them?
        //(not sure, need to see transaction spec)
      }
      Properties delegateConfig = new Properties();
      delegateConfig.putAll(baseConfig);
      if (configOverrides != null) {
        delegateConfig.putAll(configOverrides);
      }
      delegate = producerFactory.apply(delegateConfig);

      // TODO: Remove this hack when bounded flush is added to upstream
      Method producerSupportsBoundedFlush;
      try {
        producerSupportsBoundedFlush = delegate.getClass().getMethod("flush", long.class, TimeUnit.class);
      } catch (NoSuchMethodException e) {
        LOG.warn("delegate producer does not support time-bounded flush.", e);
        producerSupportsBoundedFlush = null;
      }
      boundedFlushMethod = producerSupportsBoundedFlush;

      return true;
    } finally {
      delegateLock.writeLock().unlock();
    }
  }

  @Override
  public void initTransactions() {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.initTransactions();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.beginTransaction();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.commitTransaction();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.abortTransaction();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.send(record);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      return delegate.send(record, callback);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void flush() {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      delegate.flush();
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  /**
   * A temporary hack that implements the bounded flush API defined in LiKafkaProducer.
   * TODO: remove when the bounded flush is added to upstream.
   *
   * This method will flush all the message buffered in producer. The call blocks until timeout.
   * If the underlying producer doesn't support a bounded flush, it will invoke the {@link #flush()}.
   */
  public void flush(long timeout, TimeUnit timeUnit) {
    verifyOpen();

    delegateLock.readLock().lock();
    try {
      boolean useSeparateThreadForFlush = false;
      if (boundedFlushMethod != null) {
        try {
          boundedFlushMethod.invoke(delegate, timeout, timeUnit);
        } catch (IllegalAccessException | InvocationTargetException e) {
          LOG.trace("Underlying producer does not support bounded flush!", e);
          useSeparateThreadForFlush = true;
        }
      } else {
        useSeparateThreadForFlush = true;
      }

      if (useSeparateThreadForFlush) {
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
          delegate.flush();
          latch.countDown();
        });
        t.setDaemon(true);
        t.setName(BOUNDED_FLUSH_THREAD_PREFIX + boundFlushThreadCount.getAndIncrement());
        t.setUncaughtExceptionHandler((t1, e) -> {
          LOG.warn("Thread " + t1.getName() + " terminated unexpectedly.", e);
        });
        t.start();

        boolean latchResult = false;
        try {
          latchResult = latch.await(timeout, timeUnit);
        } catch (InterruptedException e) {
          throw new InterruptException("Flush interruped.", e);
        }
        if (!latchResult) {
          throw new TimeoutException("Failed to flush accumulated records within " + timeout + " " + timeUnit);
        }
      }
    } finally {
      delegateLock.readLock().unlock();
    }
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
  public Map<MetricName, ? extends Metric> metrics() {
    //noinspection unchecked
    return (Map<MetricName, ? extends Metric>) metricsProxy;
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

  //TODO - remove after bumping up kafka-clients
  @Override
  public void close(long timeout, TimeUnit unit) {
    close(Duration.ofMillis(unit.toMillis(timeout)));
  }

  //@Override
  public void close(Duration timeout) {
    if (!proceedClosing()) {
      return;
    }
    try {
      //TODO - fix back after bumping up kafka
      delegate.close(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } finally {
      delegate = null;
      closeMdsClient();
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
   * throws an exception if this producer has been closed.
   * there's also an assumption that unless closed, delegate is always != null
   */
  private void verifyOpen() {
    if (isClosed()) {
      //same exception thats thrown by a vanilla producer
      throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }
  }
}
