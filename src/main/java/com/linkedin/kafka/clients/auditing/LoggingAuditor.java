/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.auditing;

import com.linkedin.kafka.clients.auditing.abstractImpl.AbstractAuditor;
import com.linkedin.kafka.clients.auditing.abstractImpl.AuditStats;
import com.linkedin.kafka.clients.auditing.abstractImpl.CountingAuditStats;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A simple auditor that logs the message count aggregated by time buckets.
 */
public class LoggingAuditor<K, V> extends AbstractAuditor<K, V> {
  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("AuditingLogger");

  public static final String BUCKET_MS = "auditor.bucket.ms";
  private static final String BUCKET_MS_DEFAULT = "600000";

  private long _bucketMs = -1L;

  public LoggingAuditor() {
    super();
  }

  LoggingAuditor(Time time) {
    super(time);
  }

  public void printSummary(AuditStats<K, V> auditStats) {
    CountingAuditStats<K, V> countingAuditStats = (CountingAuditStats<K, V>) auditStats;
    long bucketMs = countingAuditStats.bucketMs();
    Map<AuditType, ConcurrentSkipListMap<Long, Map<String, CountingAuditStats.AuditInfo>>> stats = countingAuditStats.stats();
    for (Map.Entry<AuditType, ConcurrentSkipListMap<Long, Map<String, CountingAuditStats.AuditInfo>>> entry : stats.entrySet()) {
      AUDIT_LOG.info("*** Audit Type: " + entry.getKey().name() + " ***");
      ConcurrentSkipListMap<Long, Map<String, CountingAuditStats.AuditInfo>> statsForType = entry.getValue();
      while (!statsForType.isEmpty()) {
        Map.Entry<Long, Map<String, CountingAuditStats.AuditInfo>> statsForBucket = statsForType.pollFirstEntry();
        String start = new Date(statsForBucket.getKey() * bucketMs).toString();
        String end = new Date(statsForBucket.getKey() * bucketMs + bucketMs).toString();
        StringBuilder builder = new StringBuilder();
        builder.append("[").append(start).append(" - ").append(end).append("] : ");
        for (Map.Entry<String, CountingAuditStats.AuditInfo> statsForTopic : statsForBucket.getValue().entrySet()) {
          builder.append(statsForTopic);
          builder.append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        AUDIT_LOG.info(builder.toString());
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _bucketMs = Long.parseLong((String) ((Map<String, Object>) configs).getOrDefault(BUCKET_MS, BUCKET_MS_DEFAULT));
  }

  @Override
  public void onTick(AuditStats<K, V> lastStats) {
    printSummary(lastStats);
  }

  @Override
  public void onClosed(AuditStats<K, V> currentStats, AuditStats<K, V> nextStats) {
    AUDIT_LOG.info("Logging Auditing stats on closure...");
    printSummary(currentStats);
    printSummary(nextStats);
  }

  @Override
  protected CountingAuditStats<K, V> newAuditStats() {
    return new CountingAuditStats<K, V>(_bucketMs);
  }
}
