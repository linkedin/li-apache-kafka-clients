/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.auditing.abstractimpl;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.auditing.LoggingAuditor;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * If auto.tick is set to false, the auditing thread would be disabled. In that case, user needs to call tick
 * manually to roll out new AuditStats.
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
 *   {@link #onClosed(AuditStats, AuditStats, long)}
 *   {@link #createAuditStats()}
 *   {@link #getAuditKey(Object, String, Long, Long, Long, AuditType)}
 *   {@link #auditToken(Object, Object)}
 * </pre>
 *
 * <p>
 * For users who wants to have customized configurations, they may override the method:
 * <pre>
 *   public void configure(Map&lt;String, ?&gt; configs);
 * </pre>
 *
 * An example implementation can be found in {@link LoggingAuditor}. The {@link LoggingAuditor} uses
 * {@link CountingAuditStats} to aggregate the audited records and simply print it to the log.
 */
public abstract class AbstractAuditor<K, V> extends Thread implements Auditor<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAuditor.class);
  // Config Names
  public static final String REPORTING_INTERVAL_MS = "auditor.reporting.interval.ms";
  public static final String REPORTING_DELAY_MS = "auditor.reporting.delay.ms";
  public static final String ENABLE_AUTO_TICK = "enable.auto.tick";

  // Config default values
  private static final String REPORTING_INTERVAL_MS_DEFAULT = "600000";
  private static final String REPORTING_DELAY_MS_DEFAULT = "60000";
  private static final String AUTO_TICK_DEFAULT = "true";

  // The timer that facilitates unit test.
  private static Time _time;

  // The logging delay in millisecond.
  private static long _reportingIntervalMs;
  // The reporting delay in millisecond. This is to tolerate some of the late arrivals.
  private static long _reportingDelayMs;
  // Whether disable the auditing thread.
  private static boolean _enableAutoTick;

  private volatile AuditStats _currentStats;
  private volatile AuditStats _nextStats;
  private volatile long _nextTick;
  private volatile long _ticks;

  // The shutdown flag and latches.
  private final AtomicBoolean _started = new AtomicBoolean(false);
  private final Object _shutdownLock = new Object();
  private volatile boolean _shutdown;
  // The shutdown flag and deadline.
  private volatile long _shutdownDeadline;

  /**
   * Construct the abstract auditor.
   */
  public AbstractAuditor() {
    super();
    this.setUncaughtExceptionHandler((t, e) -> LOG.error("Auditor encounter exception.", e));
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

  /**
   * Note that if this method is overridden by the subclasses. The overriding configure method MUST call this method
   * <b>at the end</b> to ensure the AuditStats are correctly initialized.
   *
   * @param configs The configurations for the auditor
   */
  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    _reportingIntervalMs = Long.parseLong((String) ((Map<String, Object>) configs).getOrDefault(REPORTING_INTERVAL_MS, REPORTING_INTERVAL_MS_DEFAULT));
    _reportingDelayMs = Long.parseLong((String) ((Map<String, Object>) configs).getOrDefault(REPORTING_DELAY_MS, REPORTING_DELAY_MS_DEFAULT));
    _enableAutoTick = Boolean.parseBoolean((String) ((Map<String, Object>) configs).getOrDefault(ENABLE_AUTO_TICK, AUTO_TICK_DEFAULT));
    _nextTick = _enableAutoTick ?
        (_time.milliseconds() / _reportingIntervalMs) * _reportingIntervalMs + _reportingIntervalMs : Long.MAX_VALUE;
    _ticks = 0;
    _shutdown = false;
    _shutdownDeadline = -1L;
  }

  @Override
  public void run() {
    if (_enableAutoTick) {
      LOG.info("Starting auditor...");
      try {
        while (!_shutdown) {
          try {
            long now = _time.milliseconds();
            if (now >= _nextTick + _reportingDelayMs) {
              tick();
            }
            waitForNextTick(now);
          } catch (Exception e) {
            // We catch all the exceptions from the user's onTick() call but not exit.
            LOG.error("Auditor encounter exception.", e);
          }
        }
      } finally {
        _currentStats.close();
        _nextStats.close();
        _shutdown = true;
        onClosed(_currentStats, _nextStats, Math.min(0, _shutdownDeadline - System.currentTimeMillis()));
      }
    } else {
      LOG.info("Auto auditing is set to false. Automatic ticking is disabled.");
    }
  }

  private void waitForNextTick(long now) {
    try {
      synchronized (_shutdownLock) {
        if (!_shutdown) {
          _shutdownLock.wait(Math.max(0, _nextTick + _reportingDelayMs - now));
        }
      }
    } catch (InterruptedException ie) {
      // Let it go.
    }
  }

  // protected methods
  /**
   * Get the current audit stats.
   */
  protected AuditStats currentStats() {
    return _currentStats;
  }

  /**
   * Get the next audit stats.
   */
  protected AuditStats nextStats() {
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
  protected AuditStats tickAndGetStats() {
    // We only allow one thread to tick.
    synchronized (this) {
      AuditStats prevStats = _currentStats;
      _currentStats = _nextStats;
      // Only update next tick if reporting interval is non-negative.
      if (_reportingIntervalMs >= 0) {
        _nextTick += _reportingIntervalMs;
      }
      _nextStats = createAuditStats();
      _ticks++;
      prevStats.close();
      return prevStats;
    }
  }

  /**
   * Roll out a new AuditStats and pass that to the users.
   */
  protected void tick() {
    onTick(tickAndGetStats());
  }

  /**
   * Check if the auditor is shutting down.
   *
   * @return true if the auditor is shutting down, false otherwise.
   */
  protected boolean isShuttingDown() {
    return _shutdown;
  }

  /**
   * This method is called when a reporting interval is reached and the abstract auditor rolls out a new AuditStats.
   * The old AuditStats will be closed and passed to this method as the argument. The subclass must implement this
   * method to handle the AuditStats for the previous reporting interval.
   * <p>
   *   Note that the implementation should expect to be interrupted when the auditor is shutdown and the shutdown
   *   timeout has passed.
   * </p>
   *
   * @param lastStats The AuditStats of the previous reporting interval.
   */
  public abstract void onTick(AuditStats lastStats);

  /**
   * This method will be called when the auditor is closed.
   * The AuditStats of the current and next reporting interval will be passed for the subclass to handle.
   *
   * @param currentStats The stats for the current reporting period.
   * @param nextStats The stats for the next reporting period.
   * @param timeout The timeout to close the auditor. The onClosed() call will be interrupted if it did not return
   *                before timeout.
   */
  public abstract void onClosed(AuditStats currentStats, AuditStats nextStats, long timeout);

  /**
   * Create a new AuditStats. This method will be called when the abstract auditor rolls out a new AuditStat for a new
   * reporting interval.
   */
  protected abstract AuditStats createAuditStats();

  /**
   * Get the audit key based on the record audit token. The audit key will be used to categorize the record that is
   * being audited. For example, if user wants to categorize the records based on the size of the bytes, the audit key
   * could be the combination of topic and size rounded down to 100KB. An example audit key implementation can be
   * found in {@link AuditKey}.
   *
   * @param auditToken the custom audit token extracted from key and value of the record being audited.
   * @param topic the topic of the record being audited.
   * @param timestamp the timestamp of the record being audited.
   * @param messageCount the number of records being audited.
   * @param bytesCount the number of bytes being audited.
   *
   * @return An object that can be served as an key in a {@link java.util.HashMap}. Returning null means skipping the
   * auditing.
   */
  protected abstract Object getAuditKey(Object auditToken,
                                        String topic,
                                        Long timestamp,
                                        Long messageCount,
                                        Long bytesCount,
                                        AuditType auditType);

  @Override
  public abstract Object auditToken(K key, V value);

  @Override
  public void start() {
    if (_started.compareAndSet(false, true)) {
      // Initialize the stats before starting auditor.
      _currentStats = createAuditStats();
      _nextStats = createAuditStats();
      super.start();
    }
  }

  @Override
  public void record(Object auditToken,
                     String topic,
                     Long timestamp,
                     Long messageCount,
                     Long bytesCount,
                     AuditType auditType) {
    boolean done = false;
    do {
      try {
        AuditStats auditStats = timestamp == null || timestamp >= _nextTick ? _nextStats : _currentStats;
        Object auditKey = getAuditKey(auditToken, topic, timestamp, messageCount, bytesCount, auditType);
        if (auditKey != null) {
          auditStats.update(auditKey, messageCount, bytesCount);
        }
        done = true;
      } catch (IllegalStateException ise) {
        // Ignore this exception and retry because we might be ticking.
      }
    } while (!done && !_shutdown);
  }

  @Override
  public void close(long timeout, TimeUnit unit) {
    LOG.info("Closing auditor with timeout {} {}", timeout, unit);
    long timeoutMillis = unit.toMillis(timeout);
    long now = _time.milliseconds();
    // Set shutdown flag
    synchronized (_shutdownLock) {
      if (!_shutdown) {
        // Handle long overflow
        _shutdownDeadline = Long.min(Long.MAX_VALUE - now, timeoutMillis) + now;
        _shutdown = true;
        _shutdownLock.notify();
      }
    }
    try {
      if (timeout > 0) {
        this.join(timeoutMillis);
      }
    } catch (InterruptedException e) {
      LOG.warn("Auditor closure interrupted");
    }
    // Interrupt after timeout.
    interrupt();
  }

  @Override
  public void close() {
    close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
  }
}
