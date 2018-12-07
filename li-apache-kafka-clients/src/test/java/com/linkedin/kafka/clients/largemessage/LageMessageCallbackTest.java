/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.largemessage.errors.LargeMessageSendException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * The unit test for large message callback.
 */
public class LageMessageCallbackTest {
  private final int numSegments = 10;

  @Test
  public void testLargeMessageCallbackWithoutException() {
    final AtomicInteger callbackFired = new AtomicInteger(0);
    LargeMessageCallback callback = new LargeMessageCallback(numSegments, new Callback() {
      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        callbackFired.incrementAndGet();
        assertEquals("No exception should be there.", e, null);
      }
    });

    for (int i = 0; i < numSegments - 1; i++) {
      callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0), null);
      assertTrue("The user callback should not be fired.", callbackFired.get() == 0);
    }
    callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0), null);
    assertTrue("The user callback should not be fired.", callbackFired.get() == 1);
  }

  @Test
  public void testLargeMessageCallbackWithException() {
    final AtomicInteger callbackFired = new AtomicInteger(0);
    final Exception e1 = new LargeMessageSendException("Exception 1");
    final Exception e2 = new LargeMessageSendException("Exception 2");
    LargeMessageCallback callback = new LargeMessageCallback(numSegments, new Callback() {
      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        callbackFired.incrementAndGet();
        assertTrue("The exception should be e1", e.getCause() == e1);
        assertEquals("Error when sending large message. Sent 8 of 10 segments.", e.getMessage());
      }
    });

    for (int i = 0; i < numSegments - 1; i++) {
      Exception e = null;
      if (i == 3) {
        e = e1;
      }
      callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0), e);
      assertTrue("The user callback should not be fired.", callbackFired.get() == 0);
    }
    callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0), e2);
    assertTrue("The user callback should not be fired.", callbackFired.get() == 1);
  }
}
