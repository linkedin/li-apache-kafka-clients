/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.auditing.abstractimpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class that aggregate the statistics for auditing by simply counting the number of events of different auditing
 * types for each topic.
 *
 * This class is thread safe.
 */
public class CountingAuditStats implements AuditStats {

  private final long _bucketMs;
  private final Map<Object, AuditInfo> _stats;

  // The variables for synchronization on ticks.
  private final AtomicInteger _recordingInProgress;
  private volatile boolean _closed;

  public CountingAuditStats(long bucketMs) {
    _bucketMs = bucketMs;
    _stats = new ConcurrentHashMap<>();
    _recordingInProgress = new AtomicInteger(0);
    _closed = false;
  }

  public long bucketMs() {
    return _bucketMs;
  }

  public Map<Object, AuditInfo> stats() {
    return _stats;
  }

  public void update(Object auditKey, long messageCount, long bytesCount) {
    try {
      // Increment the counter to claim usage. This is to make sure we do not close an AuditStats that is in use.
      _recordingInProgress.incrementAndGet();
      if (_closed) {
        throw new IllegalStateException("Stats has been closed. The caller should get the new AuditStats and retry.");
      }

      AuditInfo statsForTopic = _stats.get(auditKey);
      if (statsForTopic == null) {
        statsForTopic = new AuditInfo();
        AuditInfo prev = _stats.putIfAbsent(auditKey, statsForTopic);
        if (prev != null) {
          statsForTopic = prev;
        }
      }
      statsForTopic.recordMessage(messageCount, bytesCount);
    } finally {
      _recordingInProgress.decrementAndGet();
    }
  }

  public void close() {
    _closed = true;
    // We loop waiting if there is any other threads using this stats.
    // This is a spin lock, we should be able to get out of the loop pretty quickly and never end up in a tight loop.
    while (_recordingInProgress.get() > 0) { }
  }

  /**
   * A container class that hosts the messages count and bytes count for each audit key.
   */
  public static final class AuditInfo {
    private final AtomicLong _messageCount = new AtomicLong(0);
    private final AtomicLong _bytesCount = new AtomicLong(0);

    public void recordMessage(long messageCount, long bytesCount) {
      _messageCount.addAndGet(messageCount);
      _bytesCount.addAndGet(bytesCount);
    }

    public long messageCount() {
      return _messageCount.get();
    }

    public long bytesCount() {
      return _bytesCount.get();
    }

    @Override
    public String toString() {
      return "(" + _messageCount.get() + " messages, " + _bytesCount.get() + " bytes)";
    }
  }

}
