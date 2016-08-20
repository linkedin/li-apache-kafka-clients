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
import org.testng.annotations.Test;

import java.util.Map;
import com.linkedin.kafka.clients.auditing.abstractImpl.CountingAuditStats.AuditInfo;

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
    CountingAuditStats<String, String> stats = new CountingAuditStats<>(BUCKET_MS);

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

    Map<Object, AuditInfo> counters = stats.stats();
    long numBuckets = (NUM_TIMESTAMPS + BUCKET_MS - 1) / BUCKET_MS;
    long expectedNumMessages = NUM_TIMESTAMPS / numBuckets * NUM_THREAD;
    for (int typeIndex = 0; typeIndex < AUDIT_TYPES.length; typeIndex++) {
      for (int topicIndex = 0; topicIndex < TOPICS.length; topicIndex++) {
        for (long bucketIndex = 0; bucketIndex < numBuckets; bucketIndex++) {
          CountingAuditStats.AuditKey auditKey =
              new CountingAuditStats.AuditKey(TOPICS[topicIndex], bucketIndex, AUDIT_TYPES[typeIndex]);
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
    private final CountingAuditStats<String, String> _stats;

    Recorder(CountingAuditStats<String, String> stats) {
      _stats = stats;
    }

    @Override
    public void run() {
      for (int typeIndex = 0; typeIndex < AUDIT_TYPES.length; typeIndex++) {
        for (int topicIndex = 0; topicIndex < TOPICS.length; topicIndex++) {
          for (long timestamp = 0; timestamp < NUM_TIMESTAMPS; timestamp++) {
            CountingAuditStats.AuditKey auditKey =
                new CountingAuditStats.AuditKey(TOPICS[topicIndex], timestamp / BUCKET_MS, AUDIT_TYPES[typeIndex]);
            _stats.update(auditKey, 2);
          }
        }
      }
    }
  }
}
