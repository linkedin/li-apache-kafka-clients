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
import java.util.concurrent.ConcurrentSkipListMap;
import com.linkedin.kafka.clients.auditing.abstractImpl.CountingAuditStats.AuditInfo;

import static org.testng.Assert.assertEquals;

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

    Map<AuditType, ConcurrentSkipListMap<Long, Map<String, AuditInfo>>> counters = stats.stats();
    assertEquals(counters.size(), AUDIT_TYPES.length, "There should be " + AUDIT_TYPES.length + " audit types");
    for (Map.Entry<AuditType, ConcurrentSkipListMap<Long, Map<String, AuditInfo>>> entry : counters.entrySet()) {
      ConcurrentSkipListMap<Long, Map<String, AuditInfo>> statsForType = entry.getValue();
      long numBuckets = (NUM_TIMESTAMPS + BUCKET_MS - 1) / BUCKET_MS;
      assertEquals(statsForType.size(), numBuckets, "There should be " + numBuckets + " time buckets");
      long i = 0;
      while (!statsForType.isEmpty()) {
        Map.Entry<Long, Map<String, AuditInfo>> statsForBucket = statsForType.pollFirstEntry();
        assertEquals(statsForBucket.getKey().longValue(), i, "The bucket should be " + i);
        assertEquals(statsForBucket.getValue().size(), TOPICS.length, "There should be " + TOPICS.length + " topics");
        i++;
        long expectedNumMessages = NUM_TIMESTAMPS / numBuckets * NUM_THREAD;
        for (Map.Entry<String, AuditInfo> statsForTopic : statsForBucket.getValue().entrySet()) {
          assertEquals(statsForTopic.getValue().messageCount(), expectedNumMessages,
              "The topic should have " + expectedNumMessages + " messages in the bucket");
          assertEquals(statsForTopic.getValue().bytesCount(), 2 * expectedNumMessages,
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
            _stats.update(TOPICS[topicIndex], null, null, timestamp, 2, AUDIT_TYPES[typeIndex]);
          }
        }
      }
    }
  }
}
