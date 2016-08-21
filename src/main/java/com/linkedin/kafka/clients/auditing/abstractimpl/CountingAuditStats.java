/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.auditing.abstractimpl;

import com.linkedin.kafka.clients.auditing.AuditType;

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

  public void update(Object auditKey, int sizeInBytes) {
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
      statsForTopic.recordMessage(sizeInBytes);
    } finally {
      _recordingInProgress.decrementAndGet();
    }
  }

  public void close() {
    _closed = true;
    // We loop waiting if there is any other threads using this stats.
    // We should be able to get out of the loop pretty quickly
    while (_recordingInProgress.get() > 0) { }
  }

  /**
   * A container class that hosts the messages count and bytes count for each audit key.
   */
  public static class AuditInfo {
    private AtomicLong _messageCount = new AtomicLong(0);
    private AtomicLong _bytesCount = new AtomicLong(0);

    public void recordMessage(int sizeInBytes) {
      _messageCount.incrementAndGet();
      _bytesCount.addAndGet(sizeInBytes);
    }

    public long messageCount() {
      return _messageCount.get();
    }

    public long bytesCount() {
      return _bytesCount.get();
    }

    @Override
    public String toString() {
      return "(" + _messageCount.get() + "," + _bytesCount.get() + " Bytes)";
    }
  }

  /**
   * The AuditKey we defined here is simply a combination of the topic, bucket and audit type. For different use
   * cases, user may want to define a different audit key.
   */
  public static class AuditKey {
    private final String _topic;
    private final Long _bucket;
    private final AuditType _auditType;

    public AuditKey(String topic, Long bucket, AuditType auditType) {
      _topic = topic;
      _bucket = bucket;
      _auditType = auditType;
    }

    public String topic() {
      return _topic;
    }

    public Long bucket() {
      return _bucket;
    }

    public AuditType auditType() {
      return _auditType;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof AuditKey) {
        AuditKey other = (AuditKey) obj;
        return equals(_topic, other.topic()) && equals(_auditType, other.auditType());
      }
      return false;
    }

    @Override
    public int hashCode() {
      int h1 = _topic != null ? _topic.hashCode() : 0;
      int h2 = _bucket != null ? _bucket.hashCode() : 0;
      int h3 = _auditType != null ? _auditType.hashCode() : 0;
      return 31 * 31 * h1 + 31 * h2 + h3;
    }

    @Override
    public String toString() {
      return "(" + _topic + ',' + _bucket + ',' + auditType() + ')';
    }

    private static boolean equals(Object o1, Object o2) {
      if (o1 != null) {
        return o1.equals(o2);
      }
      return o2 == null;
    }

  }

}
