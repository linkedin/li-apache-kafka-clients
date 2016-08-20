/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.auditing.abstractImpl;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.auditing.LoggingAuditor;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An abstract Auditor that helps deal with tricky concurrency problems.
 *
 * The abstract auditor helps aggregate the audited records and pass the aggregated statistics to the users periodically
 * based on the reporting interval setting.
 * <p>
 * More specifically, the abstract auditor keeps an {@link AuditStats} for each reporting interval.
 * The {@link AuditStats} helps maintain the records that have been audited during the corresponding reporting interval.
 * We have provided {@link CountingAuditStats} as an implementation of {@link AuditStats} in the package.
 * The {@link CountingAuditStats} aggregates the records that have been audited into different buckets.
 * At the end of each reporting interval, the abstract auditor would tick to roll out a new {@link AuditStats} and close
 * the old audit stats. The old audit stats will then be passed to the user through {@link #onTick} method.
 *
 * <p>
 * Ideally, the timestamp of the audited records should always fall in the current reporting interval. But due to some
 * lag, it is possible that some audited records has an earlier timestamp that falls in the previous reporting interval.
 * Although this won't impact the auditing results, the abstract auditor allows user to specify a reporting.delay.ms to
 * tolerate some records that are audited later than expected. On the other hand, if a timestamp is ahead of the
 * current reporting interval, it will be aggregated into the AuditStats of next reporting interval.
 *
 * To use this class, users need to implement the following methods:
 * <pre>
 *   {@link #onTick(AuditStats)}
 *   {@link #onClosed(AuditStats, AuditStats)}
 *   {@link #newAuditStats()}
 *   {@link #getAuditKey(String, Object, Object, long, Integer, com.linkedin.kafka.clients.auditing.AuditType)}
 * </pre>
 *
 * <p>
 * For users who wants to have customized configurations, they may override the method:
 * <pre>
 *   public void configure(Map<String, ?> configs);
 * </pre>
 *
 * When users do that the {@link #configure(java.util.Map)} method must call
 * <pre>
 * super.configure(configs);
 * </pre>
 * An example implementation can be found in {@link LoggingAuditor}. The {@link LoggingAuditor} uses
 * {@link CountingAuditStats} to aggregate the audited records and simply print it to the log.
 */
public abstract class AbstractAuditor<K, V> extends Thread implements Auditor<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAuditor.class);
  // Config Names
  public static final String REPORTING_INTERVAL_MS = "auditor.reporting.interval.ms";
  public static final String REPORTING_DELAY_MS = "auditor.reporting.delay.ms";

  // Config default values
  private static final String REPORTING_INTERVAL_MS_DEFAULT = "600000";
  private static final String REPORTING_DELAY_MS_DEFAULT = "60000";

  // The number of threads holding
  // The timer that facilitates unit test.
  private static Time _time;

  // The logging delay in millisecond.
  private static long _reportingIntervalMs;
  // The logging delay in millisecond. This is to tolerate some of the late arrivals on the consumer side.
  private static long _reportingDelayMs;

  // the volatile stats.
  private volatile AuditStats<K, V> _currentStats;
  private volatile AuditStats<K, V> _nextStats;
  private volatile long _nextTick;
  private volatile long _ticks;

  // The shutdown flag and latches.
  protected volatile boolean _shutdown;

  /**
   * Construct the abstract auditor.
   */
  public AbstractAuditor() {
    super();
    _time = new SystemTime();
  }

  /**
   * Construct the abstract auditor with thread name and time object.
   *
   * @param name The auditing thread name.
   * @param time The time object.
   */
  public AbstractAuditor(String name, Time time) {
    super(name);
    _time = time;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    _reportingIntervalMs = Long.parseLong((String) ((Map<String, Object>) configs).getOrDefault(REPORTING_INTERVAL_MS, REPORTING_INTERVAL_MS_DEFAULT));
    _reportingDelayMs = Long.parseLong((String) ((Map<String, Object>) configs).getOrDefault(REPORTING_DELAY_MS, REPORTING_DELAY_MS_DEFAULT));
    _nextTick = (_time.milliseconds() / _reportingIntervalMs) * _reportingIntervalMs + _reportingIntervalMs;
    _ticks = 0;
    _shutdown = false;
  }

  @Override
  public void run() {
    try {
      while (!_shutdown) {
        long now = _time.milliseconds();
        if (now >= _nextTick + _reportingDelayMs) {
          tick();
        }
        try {
          Thread.sleep(_nextTick + _reportingDelayMs - now);
        } catch (InterruptedException ie) {
          // Let it go.
        }
      }
    } catch (Throwable t) {
      LOG.error("Logging auditor encounter exception.", t);
    } finally {
      _currentStats.close();
      _nextStats.close();
      onClosed(_currentStats, _nextStats);
    }
  }


  // protected methods
  /**
   * Get the current audit stats.
   */
  protected AuditStats<K, V> currentStats() {
    return _currentStats;
  }

  /**
   * Get the next audit stats.
   */
  protected AuditStats<K, V> nextStats() {
    return _nextStats;
  }

  /**
   * Get the time when the next tick will occur.
   */
  protected long nextTick() {
    return _nextTick;
  }

  /**
   * Get the total number of ticks occurred so far.
   */
  protected long ticks() {
    return _ticks;
  }

  /**
   * Manually tick the abstract auditor and get the AuditStats of the last reporting interval.
   */
  protected AuditStats<K, V> tickAndGetStats() {
    AuditStats<K, V> prevStats = _currentStats;
    _currentStats = _nextStats;
    _nextTick += _reportingIntervalMs;
    _nextStats = newAuditStats();
    _ticks++;
    prevStats.close();
    return prevStats;
  }

  /**
   * Roll out a new AuditStats and pass that to the users.
   */
  private void tick() {
    onTick(tickAndGetStats());
  }

  /**
   * This method is called when a reporting interval is reached and the abstract auditor rolls out a new AuditStats.
   * The old AuditStats will be closed and passed to this method as the argument. The subclass must implement this
   * method to handle the AuditStats for the previous reporting interval.
   *
   * @param lastStats The AuditStats of the previous reporting interval.
   */
  public abstract void onTick(AuditStats<K, V> lastStats);

  /**
   * This method will be called when the auditor is closed.
   * The AuditStats of the current and next reporting interval will be passed for the subclass to handle.
   *
   * @param currentStats The stats for the current reporting period.
   * @param nextStats The stats for the next reporting period.
   */
  public abstract void onClosed(AuditStats<K, V> currentStats, AuditStats<K, V> nextStats);

  /**
   * Create a new AuditStats. This method will be called when the abstract auditor rolls out a new AuditStat for a new
   * reporting interval.
   */
  protected abstract AuditStats<K, V> newAuditStats();

  /**
   * Get the audit key based on the event information. The audit key will be used to categorize the event that is
   * being audited. For example, if user wants to categorize the events based on the size of the bytes, the audit key
   * could be the combination of topic and size rounded down to 100KB. An example audit key implementation can be
   * found in {@link com.linkedin.kafka.clients.auditing.abstractImpl.CountingAuditStats.AuditKey}.
   *
   * @param topic the topic of the event being audited.
   * @param key the key of the event being audited.
   * @param value the value of the event being audited.
   * @param timestamp the timestamp of the event being audited.
   * @param sizeInBytes the size of the event being audited.
   * @param auditType the audit type of the event being audited.
   *
   * @return An object that can be served as an key in a {@link java.util.HashMap}
   */
  protected abstract Object getAuditKey(String topic,
                                        K key,
                                        V value,
                                        long timestamp,
                                        Integer sizeInBytes,
                                        AuditType auditType);

  @Override
  public void start() {
    // Initialize the stats before starting auditor.
    _currentStats = newAuditStats();
    _nextStats = newAuditStats();
    super.start();
  }

  /**
   * Package private function for unit test. Initialize the audit stats without stating the thread.
   */
  void initAuditStats() {
    _currentStats = newAuditStats();
    _nextStats = newAuditStats();
  }

  @Override
  public void record(String topic, K key, V value, Long timestamp, Integer sizeInBytes, AuditType auditType) {
    boolean done = false;
    do {
      try {
        AuditStats<K, V> auditStats = timestamp >= _nextTick ? _nextStats : _currentStats;
        auditStats.update(getAuditKey(topic, key, value, timestamp, sizeInBytes, auditType), sizeInBytes);
        done = true;
      } catch (IllegalStateException ise) {
        // Ignore this exception and retry because we might be ticking.
      }
    } while (!done && !_shutdown);
  }

  @Override
  public void close(long timeout, TimeUnit unit) {
    LOG.info("Closing auditor with timeout {} {}", timeout, unit.toString());
    _shutdown = true;
    interrupt();
    try {
      this.join(unit.toMillis(timeout));
    } catch (InterruptedException e) {
      LOG.warn("Auditor closure interrupted");
    }
  }

  @Override
  public void close() {
    close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
  }
}
