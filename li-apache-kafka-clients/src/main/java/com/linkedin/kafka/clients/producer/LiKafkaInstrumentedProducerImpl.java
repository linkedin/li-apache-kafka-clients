/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.common.InstrumentedClientLoggingHandler;
import com.linkedin.kafka.clients.common.MetricsProxy;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.mario.client.EventHandler;
import com.linkedin.mario.client.LoggingHandler;
import com.linkedin.mario.client.SimpleClient;
import com.linkedin.mario.client.SimpleClientState;
import com.linkedin.mario.common.websockets.PubSubClientType;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
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
 * @param <K> key type
 * @param <V> value type
 */
public class LiKafkaInstrumentedProducerImpl<K, V> implements DelegatingProducer<K, V>, EventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaInstrumentedProducerImpl.class);
  private static final String BOUNDED_FLUSH_THREAD_PREFIX = "Bounded-Flush-Thread-";

  private final long initialConnectionTimeoutMs;
  private final ReentrantReadWriteLock delegateLock = new ReentrantReadWriteLock();
  private final Properties baseConfig;
  private final Map<String, String> libraryVersions;
  private final ProducerFactory<K, V> producerFactory;
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
  private final String clientId; //user-provided via config.
  private final LoggingHandler loggingHandler;
  private final SimpleClient mdsClient;

  // This is null if the underlying producer does not have an implementation for time-bounded flush
  private Method boundedFlushMethod;
  private final AtomicInteger boundFlushThreadCount = new AtomicInteger();

  @Deprecated
  public LiKafkaInstrumentedProducerImpl(
      Properties baseConfig,
      ProducerFactory<K, V> producerFactory,
      Supplier<String> mdsUrlSupplier
  ) {
    this(baseConfig, null, producerFactory, mdsUrlSupplier);
  }

  public LiKafkaInstrumentedProducerImpl(
      Properties baseConfig,
      Map<String, String> libraryVersions,
      ProducerFactory<K, V> producerFactory,
      Supplier<String> mdsUrlSupplier
  ) {
    this(baseConfig, libraryVersions, producerFactory, mdsUrlSupplier, TimeUnit.SECONDS.toMillis(10));
  }

  public LiKafkaInstrumentedProducerImpl(
      Properties baseConfig,
      Map<String, String> libraryVersions,
      ProducerFactory<K, V> producerFactory,
      Supplier<String> mdsUrlSupplier,
      long initialConnectionTimeoutMs
  ) {
    List<String> conversionIssues = new ArrayList<>(1);
    this.baseConfig = baseConfig;
    Map<String, String> translatedBaseConfig = LiKafkaClientsUtils.propertiesToStringMap(baseConfig, conversionIssues);
    this.libraryVersions = LiKafkaClientsUtils.getKnownLibraryVersions();
    if (libraryVersions != null && !libraryVersions.isEmpty()) {
      this.libraryVersions.putAll(libraryVersions); //allow user arguments to override builtins
    }
    this.producerFactory = producerFactory;
    this.initialConnectionTimeoutMs = initialConnectionTimeoutMs;

    if (!conversionIssues.isEmpty()) {
      StringJoiner csv = new StringJoiner(", ");
      conversionIssues.forEach(csv::add);
      LOG.error("issues translating producer config to strings: {}", csv);
    }

    clientId = baseConfig.getProperty("client.id");
    loggingHandler = new InstrumentedClientLoggingHandler(LOG, clientId);
    mdsClient = new SimpleClient(
        "LiKafkaInstrumentedProducer " + clientId,
        PubSubClientType.PRODUCER,
        mdsUrlSupplier,
        TimeUnit.MINUTES.toMillis(1),
        TimeUnit.HOURS.toMillis(1),
        translatedBaseConfig,
        this.libraryVersions,
        this,
        loggingHandler
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
  public Producer<K, V> getDelegate() {
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
      if (isClosed()) {
        LOG.debug("this producer has been closed, not creating a new delegate");
        return false;
      }
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

      delegate = producerFactory.create(baseConfig, LiKafkaClientsUtils.convertConfigMapToProperties(configOverrides));

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

    //the callback may try and obtain a write lock (say call producer.close())
    //so we grab an update lock, which is upgradable to a write lock
    delegateLock.readLock().lock();
    try {
      return delegate.send(record, callback);
    } finally {
      delegateLock.readLock().unlock();
    }
  }

  @Override
  public void flush() {
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
    delegateLock.readLock().lock();
    try {
      boolean useSeparateThreadForFlush = false;
      if (boundedFlushMethod != null) {
        try {
          boundedFlushMethod.invoke(delegate, timeout, timeUnit);
        } catch (IllegalAccessException e1) {
          throw new IllegalStateException("Failed to invoke the bounded flush method", e1);
        } catch (InvocationTargetException e2) {
            if (e2.getCause() instanceof RuntimeException) {
              throw (RuntimeException) e2.getCause();
            } else {
              throw new IllegalStateException("Failed to invoke the bounded flush method", e2);
            }
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
      Producer<K, V> delegate = this.delegate;
      if (delegate != null) {
        delegate.close();
      }
    } finally {
      this.delegate = null;
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
      Producer<K, V> delegate = this.delegate;
      if (delegate != null) {
        //TODO - fix back after bumping up kafka
        delegate.close(timeout.toMillis(), TimeUnit.MILLISECONDS);
      }
    } finally {
      closeMdsClient();
    }
  }

  private boolean proceedClosing() {
    if (isClosed()) {
      return false;
    }

    // release read lock held by current thread if any
    // There are use cases of closing the producer in send callback if send fails, we need to ensure
    // the same ordering of acquiring/releasing locks, in this case:
    // 1. send will get the Read lock
    // 2. close will first release all Read locks; then grab the Write lock
    final int holds = delegateLock.getReadHoldCount();
    ReentrantReadWriteLock.ReadLock readLock = delegateLock.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = delegateLock.writeLock();
    if (holds > 0) { //do we own a read lock ?
      for (int i = 0; i < holds; i++) {
        readLock.unlock();
      }
      //at this point we no longer hold a read lock, but any number of other
      //readers/writers may slip past us. That's fine in dual close case since they will
      //always first try to release the read locks they own.
    }

    try {
      writeLock.lock(); //wait for a write lock
      try {
        if (isClosed()) {
          return false; //some other writer may have beaten us again
        }
        closedAt = System.currentTimeMillis();
        return true;
      } finally {
        writeLock.unlock();
      }
    } finally {
      if (holds > 0) { //restore our read lock holds (if we had any)
        for (int i = 0; i < holds; i++) {
          readLock.lock();
        }
      }
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
