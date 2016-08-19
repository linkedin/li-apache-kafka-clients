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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class that aggregate the statistics for auditing by simply counting the number of events of different auditing
 * types for each topic.
 *
 * This class is thread safe.
 */
public class CountingAuditStats<K, V> implements AuditStats<K, V> {

  private final long _bucketMs;
  private final Map<AuditType, ConcurrentSkipListMap<Long, Map<String, AuditInfo>>> _stats;

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

  public Map<AuditType, ConcurrentSkipListMap<Long, Map<String, AuditInfo>>> stats() {
    return _stats;
  }

  public void update(String topic, K key, V value, long timestamp, int sizeInBytes, AuditType auditType) {
    try {
      // Increment the counter to claim usage. This is to make sure we do not close an AnditStats that is in use.
      _recordingInProgress.incrementAndGet();
      if (_closed) {
        throw new IllegalStateException("Stats has been closed. The caller should get the new AuditStats and retry.");
      }
      ConcurrentSkipListMap<Long, Map<String, AuditInfo>> statsForType = _stats.get(auditType);
      if (statsForType == null) {
        statsForType = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Long, Map<String, AuditInfo>> prev = _stats.putIfAbsent(auditType, statsForType);
        if (prev != null) {
          statsForType = prev;
        }
      }

      long bucket = timestamp / _bucketMs;
      Map<String, AuditInfo> statsForBucket = statsForType.get(bucket);
      if (statsForBucket == null) {
        statsForBucket = new ConcurrentHashMap<>();
        Map<String, AuditInfo> prev = statsForType.putIfAbsent(bucket, statsForBucket);
        if (prev != null) {
          statsForBucket = prev;
        }
      }

      AuditInfo statsForTopic = statsForBucket.get(topic);
      if (statsForTopic == null) {
        statsForTopic = new AuditInfo();
        AuditInfo prev = statsForBucket.putIfAbsent(topic, statsForTopic);
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
    // We should be get out of the loop pretty quickly
    while (_recordingInProgress.get() > 0) { }
  }

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

}
