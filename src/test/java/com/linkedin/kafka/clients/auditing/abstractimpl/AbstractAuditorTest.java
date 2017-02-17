/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
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

  @Test
  public void testTick() {
    Time time = new MockTime();
    TestingAuditor auditor = new TestingAuditor(time);
    Map<String, String> config = new HashMap<>();
    config.put(TestingAuditor.BUCKET_MS, "30000");
    config.put(AbstractAuditor.REPORTING_DELAY_MS, "6000");
    config.put(AbstractAuditor.REPORTING_INTERVAL_MS, "60000");
    auditor.configure(config);
    auditor.start();

    assertEquals(auditor.nextTick(), 60000, "The cutting over time should be 60000");

    auditor.record(auditor.auditToken("key", "value"), TOPIC, 0L, 1L, 10L, AuditType.SUCCESS);
    auditor.record(auditor.auditToken("key", "value"), TOPIC, 30000L, 1L, 10L, AuditType.SUCCESS);
    auditor.record(auditor.auditToken("key", "value"), TOPIC, auditor.nextTick(), 1L, 10L, AuditType.SUCCESS);
    auditor.record(auditor.auditToken("key", "value"), TOPIC, auditor.nextTick(), 1L, 10L, AuditType.SUCCESS);

    assertEquals(auditor.currentStats().stats().get(new AuditKey(TOPIC, 0L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.currentStats().stats().get(new AuditKey(TOPIC, 1L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.nextStats().stats().get(new AuditKey(TOPIC, 2L, AuditType.SUCCESS)).messageCount(), 2,
        "There should be two messages in bucket 2 in the next stats");

    // Advance the clock to 1 ms before next tick.
    time.sleep(59999);
    long ticks = auditor.ticks();
    auditor.interrupt();
    long startMs = System.currentTimeMillis();
    while (auditor.ticks() != ticks + 1 && System.currentTimeMillis() < startMs + 5000) { }
    assertEquals(auditor.currentStats().stats().get(new AuditKey(TOPIC, 0L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.currentStats().stats().get(new AuditKey(TOPIC, 1L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.nextStats().stats().get(new AuditKey(TOPIC, 2L, AuditType.SUCCESS)).messageCount(), 2,
        "There should be two messages in bucket 2 in the next stats");

    // Advance the clock again to the nextTick + REPORTING_DELAY_MS - 1. The tick should not happen due to the logging delay.
    time.sleep(6000);
    ticks = auditor.ticks();
    auditor.interrupt();
    startMs = System.currentTimeMillis();
    while (auditor.ticks() != ticks + 1 && System.currentTimeMillis() < startMs + 5000) { }
    assertEquals(auditor.currentStats().stats().get(new AuditKey(TOPIC, 0L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.currentStats().stats().get(new AuditKey(TOPIC, 1L, AuditType.SUCCESS)).messageCount(), 1,
        "There should be one message in the current stats");
    assertEquals(auditor.nextStats().stats().get(new AuditKey(TOPIC, 2L, AuditType.SUCCESS)).messageCount(), 2,
        "There should be two messages in bucket 2 in the next stats");

    // Advance the clock again, now it should tick.
    time.sleep(1);
    ticks = auditor.ticks();
    auditor.interrupt();
    startMs = System.currentTimeMillis();
    while (auditor.ticks() != ticks + 1 && System.currentTimeMillis() < startMs + 5000) { }
    assertEquals(auditor.currentStats().stats().get(new AuditKey(TOPIC, 2L, AuditType.SUCCESS)).messageCount(), 2,
        "There should be one message in the current stats");
    assertTrue(auditor.nextStats().stats().isEmpty(), "The next stats should be empty now.");

    auditor.close();
  }

  @Test
  public void testClose() {
    Time time = new MockTime();
    AbstractAuditor<String, String> auditor = new TestingAuditor(time);
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

    Time time = new MockTime();
    TestingAuditor auditor = new TestingAuditor(time);
    Map<String, String> config = new HashMap<>();
    config.put(TestingAuditor.BUCKET_MS, "1000");
    config.put(AbstractAuditor.REPORTING_DELAY_MS, "100");
    config.put(AbstractAuditor.REPORTING_INTERVAL_MS, "1000");
    config.put(AbstractAuditor.ENABLE_AUTO_TICK, "false");
    auditor.configure(config);
    auditor.start();

    for (int i = 0; i < recorders.length; i++) {
      recorders[i] = new Recorder(auditor, numTimestamps, topics, auditTypes);
      recorders[i].start();
    }

    // Let's tick.
    CountingAuditStats stats = null;
    Map<AuditKey, AtomicLong> counters = new HashMap<>();
    long startMs = System.currentTimeMillis();
    long totalCount = 0;
    while (totalCount < numTimestamps * topics.length * auditTypes.length * numThreads
        && System.currentTimeMillis() < startMs + 30000) {
      stats = auditor.tickAndGetStats();
      // Add the stats
      for (Map.Entry<Object, CountingAuditStats.AuditingCounts> entry : stats.stats().entrySet()) {
        AuditKey auditKey = (AuditKey) entry.getKey();
        CountingAuditStats.AuditingCounts auditingCounts = entry.getValue();
        counters.putIfAbsent(auditKey, new AtomicLong(0));
        counters.get(auditKey).addAndGet(auditingCounts.messageCount());
        totalCount += auditingCounts.messageCount();
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
          AuditKey auditKey =
              new AuditKey(topics[topicIndex], bucketIndex, auditTypes[typeIndex]);
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
            _auditor.record(_auditor.auditToken("key", "value"), _topics[topicIndex], timestamp, 1L, 2L, _auditTypes[typeIndex]);
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
    protected AuditStats createAuditStats() {
      return new CountingAuditStats(_bucketMs);
    }

    @Override
    public Object auditToken(String key, String value) {
      return null;
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
    public long hiResClockMs() {
      return now;
    }

    @Override
    public void sleep(long ms) {
      now += ms;
    }
  }
}
