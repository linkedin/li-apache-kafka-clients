/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.auditing;

import com.linkedin.kafka.clients.auditing.abstractimpl.AbstractAuditor;
import com.linkedin.kafka.clients.auditing.abstractimpl.AuditKey;
import com.linkedin.kafka.clients.auditing.abstractimpl.AuditStats;
import com.linkedin.kafka.clients.auditing.abstractimpl.CountingAuditStats;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

/**
 * A simple auditor that logs the message count aggregated by time buckets.
 */
public class LoggingAuditor<K, V> extends AbstractAuditor<K, V> {
  private static final Logger AUDIT_LOG = LoggerFactory.getLogger(LoggingAuditor.class);

  public static final String BUCKET_MS = "auditor.bucket.ms";
  private static final String BUCKET_MS_DEFAULT = "600000";

  private long _bucketMs = -1L;

  public LoggingAuditor() {
    super();
  }

  public LoggingAuditor(String name, Time time) {
    super(name, time);
  }

  public void printSummary(AuditStats auditStats) {
    CountingAuditStats countingAuditStats = (CountingAuditStats) auditStats;
    long bucketMs = countingAuditStats.bucketMs();
    Map<Object, CountingAuditStats.AuditingCounts> stats = countingAuditStats.stats();
    for (Map.Entry<Object, CountingAuditStats.AuditingCounts> entry : stats.entrySet()) {
      AuditKey auditKey = (AuditKey) entry.getKey();
      CountingAuditStats.AuditingCounts auditingCounts = entry.getValue();
      String start = new Date(auditKey.bucket() * bucketMs).toString();
      String end = new Date(auditKey.bucket() * bucketMs + bucketMs).toString();
      AUDIT_LOG.info("[{} - {}] : {}, {}", start, end, auditKey, auditingCounts);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _bucketMs = Long.parseLong((String) ((Map<String, Object>) configs).getOrDefault(BUCKET_MS, BUCKET_MS_DEFAULT));
  }

  @Override
  public void onTick(AuditStats lastStats) {
    printSummary(lastStats);
  }

  @Override
  public void onClosed(AuditStats currentStats, AuditStats nextStats, long timeout) {
    AUDIT_LOG.info("Logging auditing stats on closure...");
    printSummary(currentStats);
    printSummary(nextStats);
  }

  @Override
  protected CountingAuditStats createAuditStats() {
    return new CountingAuditStats(_bucketMs);
  }

  @Override
  protected Object getAuditKey(Object auditToken,
                               String topic,
                               Long timestamp,
                               Long messageCount,
                               Long sizeInBytes,
                               AuditType auditType) {
    return new AuditKey(topic, timestamp / _bucketMs, auditType);
  }

  @Override
  public Object auditToken(K key, V value) {
    // The logging auditor does not have custom audit information.
    return null;
  }
}
