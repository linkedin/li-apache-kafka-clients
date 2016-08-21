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
import com.linkedin.kafka.clients.auditing.Auditor;
import org.apache.kafka.common.utils.Time;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * The unit test for AbstractAuditor.
 */
public class AbstractAuditorTest {
  private static final String TOPIC = "topic";
  private static final Time TIME = new MockTime();

  @Test
  public void testTick() {
    TestingAuditor auditor = new TestingAuditor(TIME);
    Map<String, String> config = new HashMap<>();
    config.put(TestingAuditor.BUCKET_MS, "30000");
    config.put(AbstractAuditor.REPORTING_DELAY_MS, "6000");
    config.put(AbstractAuditor.REPORTING_INTERVAL_MS, "60000");
    auditor.configure(config);
    auditor.start();

    assertEquals(auditor.nextTick(), 60000, "The cutting over time should be 60000");

    auditor.record(TOPIC, "key", "value", 0L, 10, AuditType.SUCCESS);
    auditor.record(TOPIC, "key", "value", 30000L, 10, AuditType.SUCCESS);
    auditor.record(TOPIC, "key", "value", auditor.nextTick(), 10, AuditType.SUCCESS);
    auditor.record(TOPIC, "key", "value", auditor.nextTick(), 10, AuditType.SUCCESS);

    assertEquals(auditor.currentStats().stats().get(new CountingAuditStats.AuditKey(TOPIC, 0L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.currentStats().stats().get(new CountingAuditStats.AuditKey(TOPIC, 1L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.nextStats().stats().get(new CountingAuditStats.AuditKey(TOPIC, 2L, AuditType.SUCCESS)).messageCount(), 2,
        "There should be two messages in bucket 2 in the next stats");

    // Advance the clock to 1 ms before next tick.
    TIME.sleep(59999);
    auditor.interrupt();
    long ticks = auditor.ticks();
    long startMs = System.currentTimeMillis();
    while (auditor.ticks() != ticks + 1 && System.currentTimeMillis() < startMs + 5000) { }
    assertEquals(auditor.currentStats().stats().get(new CountingAuditStats.AuditKey(TOPIC, 0L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.currentStats().stats().get(new CountingAuditStats.AuditKey(TOPIC, 1L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.nextStats().stats().get(new CountingAuditStats.AuditKey(TOPIC, 2L, AuditType.SUCCESS)).messageCount(), 2,
        "There should be two messages in bucket 2 in the next stats");

    // Advance the clock again to the nextTick + REPORTING_DELAY_MS - 1. The tick should not happen due to the logging delay.
    TIME.sleep(6000);
    auditor.interrupt();
    ticks = auditor.ticks();
    startMs = System.currentTimeMillis();
    while (auditor.ticks() != ticks + 1 && System.currentTimeMillis() < startMs + 5000) { }
    assertEquals(auditor.currentStats().stats().get(new CountingAuditStats.AuditKey(TOPIC, 0L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.currentStats().stats().get(new CountingAuditStats.AuditKey(TOPIC, 1L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.nextStats().stats().get(new CountingAuditStats.AuditKey(TOPIC, 2L, AuditType.SUCCESS)).messageCount(), 2,
        "There should be two messages in bucket 2 in the next stats");

    // Advance the clock again, now it should tick.
    TIME.sleep(1);
    auditor.interrupt();
    ticks = auditor.ticks();
    startMs = System.currentTimeMillis();
    while (auditor.ticks() != ticks + 1 && System.currentTimeMillis() < startMs + 5000) { }
    assertEquals(auditor.currentStats().stats().get(new CountingAuditStats.AuditKey(TOPIC, 2L, AuditType.SUCCESS)).messageCount(), 2,
        "There should be one message in the current stats");
    assertTrue(auditor.nextStats().stats().isEmpty(), "The next stats should be empty now.");

    auditor.close();
  }

  @Test
  public void testClose() {
    AbstractAuditor<String, String> auditor = new TestingAuditor(TIME);
    Map<String, String> config = new HashMap<>();
    config.put(TestingAuditor.BUCKET_MS, "30000");
    config.put(AbstractAuditor.REPORTING_DELAY_MS, "6000");
    config.put(AbstractAuditor.REPORTING_INTERVAL_MS, "60000");
    auditor.configure(config);
    auditor.start();

    auditor.close();

    assertEquals(auditor.getState(), Thread.State.TERMINATED, "The auditor thread should have exited.");
  }

  /**
   * The test runs 5 recorder threads. The main thread keeps ticking manually to see if there would be race condition
   * that causes the counts mismatch.
   */
  @Test
  public void testConcurrency() {
    final int numThreads = 5;
    final int numTimestamps = 10000;
    final long bucketMs = 1000;
    final String[] topics = {"topic0", "topic1", "topic2"};
    final AuditType[] auditTypes = {AuditType.SUCCESS, AuditType.FAILURE, AuditType.ATTEMPT};
    Recorder[] recorders = new Recorder[5];

    TestingAuditor auditor = new TestingAuditor(TIME);
    Map<String, String> config = new HashMap<>();
    config.put(TestingAuditor.BUCKET_MS, "1000");
    config.put(AbstractAuditor.REPORTING_DELAY_MS, "100");
    config.put(AbstractAuditor.REPORTING_INTERVAL_MS, "10000");
    auditor.configure(config);
    // Do not start auditor, we will tick manually.
    auditor.initAuditStats();

    for (int i = 0; i < recorders.length; i++) {
      recorders[i] = new Recorder(auditor, numTimestamps, topics, auditTypes);
      recorders[i].start();
    }

    // Let's tick. We will tick at most 10 times.
    CountingAuditStats stats = null;
    Map<CountingAuditStats.AuditKey, AtomicLong> counters = new HashMap<>();
    boolean done = false;
    while (!done) {
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      stats = auditor.tickAndGetStats();
      if (stats.stats().size() == 0) {
        done = true;
      }
      // Add the stats
      for (Map.Entry<Object, CountingAuditStats.AuditInfo> entry : stats.stats().entrySet()) {
        CountingAuditStats.AuditKey auditKey = (CountingAuditStats.AuditKey) entry.getKey();
        CountingAuditStats.AuditInfo auditInfo = entry.getValue();
        counters.putIfAbsent(auditKey, new AtomicLong(0));
        counters.get(auditKey).addAndGet(auditInfo.messageCount());
      }
    }
    auditor.close();

    for (int i = 0; i < recorders.length; i++) {
      try {
        recorders[i].join();
      } catch (InterruptedException e) {
        // Let it go.
      }
    }
    assertTrue(auditor.ticks() > 2);

    // Now verify
    long numBuckets = (numTimestamps + bucketMs - 1) / bucketMs;
    long expectedNumMessages = numTimestamps / numBuckets * numThreads;
    for (int typeIndex = 0; typeIndex < auditTypes.length; typeIndex++) {
      for (int topicIndex = 0; topicIndex < topics.length; topicIndex++) {
        for (long bucketIndex = 0; bucketIndex < numBuckets; bucketIndex++) {
          CountingAuditStats.AuditKey auditKey =
              new CountingAuditStats.AuditKey(topics[topicIndex], bucketIndex, auditTypes[typeIndex]);
          assertTrue(counters.containsKey(auditKey));
          assertEquals(counters.get(auditKey).longValue(), expectedNumMessages,
              "The topic should have " + expectedNumMessages + " messages in the bucket");
        }
      }
    }
  }

  private static class Recorder extends Thread {
    private final int _numTimestamps;
    private final String[] _topics;
    private final AuditType[] _auditTypes;
    private final Auditor<String, String> _auditor;

    Recorder(Auditor<String, String> auditor, int numTimestamps, String[] topics, AuditType[] auditTypes) {
      _auditor = auditor;
      _numTimestamps = numTimestamps;
      _topics = topics;
      _auditTypes = auditTypes;
    }

    @Override
    public void run() {
      for (int typeIndex = 0; typeIndex < _auditTypes.length; typeIndex++) {
        for (int topicIndex = 0; topicIndex < _topics.length; topicIndex++) {
          for (long timestamp = 0; timestamp < _numTimestamps; timestamp++) {
            _auditor.record(_topics[topicIndex], "key", "value", timestamp, 2, _auditTypes[typeIndex]);
          }
        }
      }
    }
  }

  private static class TestingAuditor extends AbstractAuditor<String, String> {
    public static final String BUCKET_MS = "bucket.ms";
    private long _bucketMs;

    TestingAuditor(Time time) {
      super("test-auditor", time);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
      super.configure(configs);
      _bucketMs = Long.parseLong((String) ((Map<String, Object>) configs).getOrDefault(BUCKET_MS, "30000"));
    }

    // protected methods for unit test.
    public CountingAuditStats currentStats() {
      return (CountingAuditStats) super.currentStats();
    }

    public CountingAuditStats nextStats() {
      return (CountingAuditStats) super.nextStats();
    }

    public long nextTick() {
      return super.nextTick();
    }

    public long ticks() {
      return super.ticks();
    }

    public CountingAuditStats tickAndGetStats() {
      return (CountingAuditStats) super.tickAndGetStats();
    }

    @Override
    public void onTick(AuditStats lastStats) {

    }

    @Override
    public void onClosed(AuditStats currentStats, AuditStats nextStats) {

    }

    @Override
    protected AuditStats newAuditStats() {
      return new CountingAuditStats(_bucketMs);
    }

    @Override
    protected Object getAuditKey(String topic,
                                 String key,
                                 String value,
                                 long timestamp,
                                 Integer sizeInBytes,
                                 AuditType auditType) {
      return new CountingAuditStats.AuditKey(topic, timestamp / _bucketMs, auditType);
    }
  }

  private static class MockTime implements Time {

    volatile long now = 0L;

    @Override
    public long milliseconds() {
      return now;
    }

    @Override
    public long nanoseconds() {
      // Not used.
      return 0;
    }

    @Override
    public void sleep(long ms) {
      now += ms;
    }
  }
}
