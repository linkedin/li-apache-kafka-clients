/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.auditing.helper;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An abstract Auditor that helps deal with tricky concurrency problems.
 */
public abstract class AbstractAuditor<K, V> extends Thread implements Auditor<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAuditor.class);
  // Config Names
  public static final String BUCKET_MS = "auditor.bucket.ms";
  public static final String REPORTING_INTERVAL_MS = "auditor.reporting.interval.ms";
  public static final String REPORTING_DELAY_MS = "auditor.reporting.delay.ms";

  // Config default values
  private static final String BUCKET_MS_DEFAULT = "600000";
  private static final String REPORTING_INTERVAL_MS_DEFAULT = "600000";
  private static final String REPORTING_DELAY_MS_DEFAULT = "60000";

  // The number of threads holding
  // The timer that facilitates unit test.
  private static Time _time;

  // The auditing time bucket in millisecond.
  private static long _bucketMs;
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

  public AbstractAuditor() {
    super();
    _time = new SystemTime();
  }

  // Protected constructor for unit test.
  protected AbstractAuditor(Time time) {
    super();
    _time = time;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    _bucketMs = Long.parseLong((String) ((Map<String, Object>) configs).getOrDefault(BUCKET_MS, BUCKET_MS_DEFAULT));
    _reportingIntervalMs = Long.parseLong((String) ((Map<String, Object>) configs).getOrDefault(REPORTING_INTERVAL_MS, REPORTING_INTERVAL_MS_DEFAULT));
    _reportingDelayMs = Long.parseLong((String) ((Map<String, Object>) configs).getOrDefault(REPORTING_DELAY_MS, REPORTING_DELAY_MS_DEFAULT));
    _currentStats = newAuditStats(_bucketMs);
    _nextStats = newAuditStats(_bucketMs);
    _nextTick = (_time.milliseconds() / _bucketMs) * _bucketMs + _reportingIntervalMs;
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
  protected AuditStats<K, V> currentStats() {
    return _currentStats;
  }

  protected AuditStats<K, V> nextStats() {
    return _nextStats;
  }

  protected long nextTick() {
    return _nextTick;
  }

  protected long ticks() {
    return _ticks;
  }

  protected AuditStats<K, V> tickAndGetStats() {
    AuditStats<K, V> prevStats = _currentStats;
    _currentStats = _nextStats;
    _nextTick += _reportingIntervalMs;
    _nextStats = newAuditStats(_bucketMs);
    _ticks++;
    prevStats.close();
    return prevStats;
  }

  private void tick() {
    onTick(tickAndGetStats());
  }

  public abstract void onTick(AuditStats<K, V> lastStats);

  public abstract void onClosed(AuditStats<K, V> currentStats, AuditStats<K, V> nextStats);

  protected abstract AuditStats<K, V> newAuditStats(long bucketMs);

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void record(String topic, K key, V value, Long timestamp, Integer sizeInBytes, AuditType auditType) {
    boolean done = false;
    do {
      try {
        AuditStats<K, V> auditStats = timestamp >= _nextTick ? _nextStats : _currentStats;
        auditStats.update(topic, key, value, timestamp, sizeInBytes, auditType);
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
