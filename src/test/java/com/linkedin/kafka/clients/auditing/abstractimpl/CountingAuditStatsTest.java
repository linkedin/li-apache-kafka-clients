/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.auditing.abstractimpl;

import com.linkedin.kafka.clients.auditing.AuditType;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * The unit test for AuditStats
 */
public class CountingAuditStatsTest {

  private static final int NUM_THREAD = 5;
  private static final int NUM_TIMESTAMPS = 10000;
  private static final long BUCKET_MS = 1000L;
  private static final String[] TOPICS = {"topic0", "topic1", "topic2"};
  private static final AuditType[] AUDIT_TYPES = {AuditType.SUCCESS, AuditType.FAILURE, AuditType.ATTEMPT};

  @Test
  public void testRecord() {
    Thread[] recorders = new Thread[NUM_THREAD];
    CountingAuditStats stats = new CountingAuditStats(BUCKET_MS);

    for (int i = 0; i < NUM_THREAD; i++) {
      recorders[i] = new Recorder(stats);
      recorders[i].start();
    }

    for (int i = 0; i < NUM_THREAD; i++) {
      try {
        recorders[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    Map<Object, CountingAuditStats.AuditingCounts> counters = stats.stats();
    long numBuckets = (NUM_TIMESTAMPS + BUCKET_MS - 1) / BUCKET_MS;
    long expectedNumMessages = NUM_TIMESTAMPS / numBuckets * NUM_THREAD;
    for (int typeIndex = 0; typeIndex < AUDIT_TYPES.length; typeIndex++) {
      for (int topicIndex = 0; topicIndex < TOPICS.length; topicIndex++) {
        for (long bucketIndex = 0; bucketIndex < numBuckets; bucketIndex++) {
          AuditKey auditKey =
              new AuditKey(TOPICS[topicIndex], bucketIndex, AUDIT_TYPES[typeIndex]);
          assertTrue(counters.containsKey(auditKey));
          assertEquals(counters.get(auditKey).messageCount(), expectedNumMessages,
              "The topic should have " + expectedNumMessages + " messages in the bucket");
          assertEquals(counters.get(auditKey).bytesCount(), 2 * expectedNumMessages,
              "The topic should have " + 2 * expectedNumMessages + " messages in the bucket");
        }
      }
    }
  }

  private class Recorder extends Thread {
    private final CountingAuditStats _stats;

    Recorder(CountingAuditStats stats) {
      _stats = stats;
    }

    @Override
    public void run() {
      for (int typeIndex = 0; typeIndex < AUDIT_TYPES.length; typeIndex++) {
        for (int topicIndex = 0; topicIndex < TOPICS.length; topicIndex++) {
          for (long timestamp = 0; timestamp < NUM_TIMESTAMPS; timestamp++) {
            AuditKey auditKey =
                new AuditKey(TOPICS[topicIndex], timestamp / BUCKET_MS, AUDIT_TYPES[typeIndex]);
            _stats.update(auditKey, 1, 2);
          }
        }
      }
    }
  }
}
