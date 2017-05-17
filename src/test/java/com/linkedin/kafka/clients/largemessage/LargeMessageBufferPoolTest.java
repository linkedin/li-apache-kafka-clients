/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.largemessage.errors.InvalidSegmentException;
import com.linkedin.kafka.clients.largemessage.errors.LargeMessageDroppedException;
import com.linkedin.kafka.clients.utils.LiKafkaClientsTestUtils;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Unit test for IncompleteMessageBufferPool
 */
public class LargeMessageBufferPoolTest {

  @Test
  public void testBasicAssemble() {
    // Create a message pool with 30 bytes capacity and expiration offset gap = 20.
    LargeMessageBufferPool pool = new LargeMessageBufferPool(30, 20, true);

    TopicPartition tp = new TopicPartition("topic", 0);
    UUID messageId0 = LiKafkaClientsUtils.randomUUID();

    long offset = 0;
    LargeMessageSegment m0Seg0 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 0, 3, 25, 10);
    LargeMessageSegment m0Seg1 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 1, 3, 25, 10);
    LargeMessageSegment m0Seg2 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 2, 3, 25, 5);

    // Step 1: insert message 0
    assertEquals(pool.tryCompleteMessage(tp, offset++, m0Seg0).serializedMessage(), null, "No message should be completed");
    assertEquals(pool.size(), 1, "Buffer pool size should be 1.");
    assertEquals(pool.bufferUsed(), 10, "Buffer pool buffered bytes should be 10.");
    assertEquals(pool.safeOffsets().size(), 1, "Safe offset map size should 1.");
    assertEquals(pool.safeOffsets().get(tp).longValue(), 0, "Safe offset for partition 0 should be 0.");

    assertEquals(pool.tryCompleteMessage(tp, offset++, m0Seg1).serializedMessage(), null, "No message should be completed");
    assertEquals(pool.size(), 1, "Buffer pool size should be 1.");
    assertEquals(pool.bufferUsed(), 20, "Buffer pool buffered bytes should be 20.");
    assertEquals(pool.safeOffsets().size(), 1, "Safe offset map size should 1.");
    assertEquals(pool.safeOffsets().get(tp).longValue(), 0, "Safe offset for partition 0 should be 0.");

    assertEquals(pool.tryCompleteMessage(tp, offset++, m0Seg1).serializedMessage(), null, "No message should be completed on duplicates");
    assertEquals(pool.size(), 1, "Buffer pool size should be 1.");
    assertEquals(pool.bufferUsed(), 20, "Buffer pool buffered bytes should be 20.");
    assertEquals(pool.safeOffsets().size(), 1, "Safe offset map size should 1.");
    assertEquals(pool.safeOffsets().get(tp).longValue(), 0, "Safe offset for partition 0 should be 0.");

    LargeMessage.SegmentAddResult segmentAddResult = pool.tryCompleteMessage(tp, offset, m0Seg2);
    assertNotNull(segmentAddResult.serializedMessage(), "Message 0 should be completed.");
    assertEquals(segmentAddResult.startingOffset(), 0, "Message starting offset should be 0");

    LiKafkaClientsTestUtils.verifyMessage(segmentAddResult.serializedMessage(), 25, 10);
  }

  @Test
  public void testExceptionOnMessageDropped() {
    // Create two message pools with 50 bytes capacity and expiration offset gap = 20.
    LargeMessageBufferPool pool0 = new LargeMessageBufferPool(50, 20, true);
    LargeMessageBufferPool pool1 = new LargeMessageBufferPool(50, 20, false);

    TopicPartition tp = new TopicPartition("topic", 0);
    UUID messageId0 = LiKafkaClientsUtils.randomUUID();
    UUID messageId1 = LiKafkaClientsUtils.randomUUID();

    long offset = 0;
    LargeMessageSegment m0Seg0 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 0, 3, 50, 20);
    LargeMessageSegment m0Seg1 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 1, 3, 50, 20);
    LargeMessageSegment m1Seg0 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId1, 0, 3, 25, 20);

    assertEquals(pool0.tryCompleteMessage(tp, offset, m0Seg0).serializedMessage(), null, "No message should be completed");
    assertEquals(pool1.tryCompleteMessage(tp, offset++, m0Seg0).serializedMessage(), null, "No message should be completed");

    assertEquals(pool0.tryCompleteMessage(tp, offset, m0Seg1).serializedMessage(), null, "No message should be completed");
    assertEquals(pool1.tryCompleteMessage(tp, offset++, m0Seg1).serializedMessage(), null, "No message should be completed");

    assertEquals(pool0.bufferUsed(), 40, "Buffer pool0 buffered bytes should be 40.");
    assertEquals(pool1.bufferUsed(), 40, "Buffer pool1 buffered bytes should be 40.");
    try {
      pool0.tryCompleteMessage(tp, offset, m1Seg0);
      fail("pool0 should throw LargeMessageException due to message dropped.");
    } catch (LargeMessageDroppedException lmde) {
      assertTrue(lmde.getMessage().startsWith("The following large Message is dropped due to buffer full"));
    }
    assertEquals(pool0.bufferUsed(), 0, "Buffer pool0 buffered bytes should be 0.");
    // Now pool0 should have enough space to process  message1 again.
    pool0.tryCompleteMessage(tp, offset, m1Seg0);
    pool1.tryCompleteMessage(tp, offset, m1Seg0);

    assertEquals(pool0.bufferUsed(), 20, "Buffer pool0 buffered bytes should be 20.");
    assertEquals(pool1.bufferUsed(), 20, "Buffer pool1 buffered bytes should be 20.");
  }

  @Test
  public void testEvictionAndSafeOffsets() {
    // Create an assembler with 50 bytes capacity and expiration offset gap = 20.
    LargeMessageBufferPool pool = new LargeMessageBufferPool(30, 20, false);

    TopicPartition tp = new TopicPartition("topic", 0);
    UUID messageId0 = LiKafkaClientsUtils.randomUUID();
    UUID messageId1 = LiKafkaClientsUtils.randomUUID();
    UUID messageId2 = LiKafkaClientsUtils.randomUUID();
    long offset = 0;
    LargeMessageSegment m0Seg0 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 0, 3, 25, 10);
    LargeMessageSegment m0Seg1 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 1, 3, 25, 10);
    LargeMessageSegment m0Seg2 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 3, 3, 25, 5);
    LargeMessageSegment m1Seg0 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId1, 0, 3, 30, 10);
    LargeMessageSegment m1Seg1 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId1, 1, 3, 30, 10);
    LargeMessageSegment m2Seg0 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId2, 0, 3, 30, 10);
    LargeMessageSegment m2Seg1 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId2, 1, 3, 30, 10);
    LargeMessageSegment m2Seg2 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId2, 2, 3, 30, 10);

    // Step 1: insert message 0
    assertEquals(pool.tryCompleteMessage(tp, offset++, m0Seg0).serializedMessage(), null, "No message should be completed");
    assertEquals(pool.size(), 1, "Buffer pool size should be 1.");
    assertEquals(pool.bufferUsed(), 10, "Buffer pool buffered bytes should be 10.");
    assertEquals(pool.safeOffsets().size(), 1, "Safe offset map size should 1.");
    assertEquals(pool.safeOffsets().get(tp).longValue(), 0, "Safe offset for partition 0 should be 0.");

    assertEquals(pool.tryCompleteMessage(tp, offset++, m0Seg1).serializedMessage(), null, "No message should be completed");
    assertEquals(pool.size(), 1, "Buffer pool size should be 1.");
    assertEquals(pool.bufferUsed(), 20, "Buffer pool buffered bytes should be 20.");
    assertEquals(pool.safeOffsets().size(), 1, "Safe offset map size should 1.");
    assertEquals(pool.safeOffsets().get(tp).longValue(), 0, "Safe offset for partition 0 should be 0.");

    // step 2: insert message 1 to evict message 0 for free space.
    // offset = 2
    assertEquals(pool.tryCompleteMessage(tp, offset++, m1Seg1).serializedMessage(), null, "No message should be completed");
    assertEquals(pool.size(), 2, "Buffer pool size should be 2.");
    assertEquals(pool.bufferUsed(), 30, "Buffer pool buffered bytes should be 30.");
    assertEquals(pool.safeOffsets().size(), 1, "Safe offset map size should be 1.");
    assertEquals(pool.safeOffsets().get(tp).longValue(), 0, "Safe offset for partition 0 should be 0.");

    // offset = 3
    assertEquals(pool.tryCompleteMessage(tp, offset, m1Seg0).serializedMessage(), null, "No message should be completed");
    assertEquals(pool.size(), 1, "Buffer pool should only contain message 1.");
    assertEquals(pool.bufferUsed(), 20, "Buffer pool buffered bytes should be 20.");
    assertEquals(pool.safeOffsets().size(), 1, "Safe offset map size should 1.");
    assertEquals(pool.safeOffsets().get(tp).longValue(), 2, "Safe offset for partition 0 should be 2.");

    // Step 3: Insert message 2 to evict message 1 due to offset gap.
    offset = 30;
    assertEquals(pool.tryCompleteMessage(tp, offset++, m2Seg0).serializedMessage(), null, "No message should be completed");
    assertEquals(pool.size(), 1, "Buffer pool should only contain message 2.");
    assertEquals(pool.bufferUsed(), 10, "Buffer pool buffered bytes should be 10.");
    assertEquals(pool.safeOffsets().size(), 1, "Safe offset map size should 1.");
    assertEquals(pool.safeOffsets().get(tp).longValue(), 30, "Safe offset for partition 0 should be 30.");

    assertEquals(pool.tryCompleteMessage(tp, offset++, m2Seg1).serializedMessage(), null, "No message should be completed");
    byte[] serializedMessage2 = pool.tryCompleteMessage(tp, offset, m2Seg2).serializedMessage();
    assertNotNull(serializedMessage2, "Message 2 should be completed");
    assertEquals(pool.bufferUsed(), 0, "No message should be in the pool");
    assertEquals(pool.safeOffsets().size(), 0, "Safe offset map should be empty.");

    LiKafkaClientsTestUtils.verifyMessage(serializedMessage2, 30, 10);
  }

  @Test
  public void testSequenceNumberOutOfRange() {
    UUID messageId = LiKafkaClientsUtils.randomUUID();
    TopicPartition tp = new TopicPartition("topic", 0);
    LargeMessageBufferPool pool = new LargeMessageBufferPool(30, 20, false);
    LargeMessageSegment segment = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId, 3, 3, 25, 5);
    try {
      pool.tryCompleteMessage(tp, 0, segment);
      fail("Should throw large message exception for sequence number out of range.");
    } catch (InvalidSegmentException ise) {
      assertTrue(ise.getMessage().startsWith("Sequence number"));
    }
  }

  @Test
  public void testNullMessageId() {
    TopicPartition tp = new TopicPartition("topic", 0);
    LargeMessageBufferPool pool = new LargeMessageBufferPool(30, 20, false);
    LargeMessageSegment segment = LiKafkaClientsTestUtils.createLargeMessageSegment(null, 2, 3, 25, 5);
    try {
      pool.tryCompleteMessage(tp, 0, segment);
      fail("Should throw large message exception for null message id.");
    } catch (InvalidSegmentException ise) {
      assertTrue(ise.getMessage().startsWith("Message Id"));
    }
  }

  @Test
  public void testSegmentSizeTooLarge() {
    UUID messageId = LiKafkaClientsUtils.randomUUID();
    TopicPartition tp = new TopicPartition("topic", 0);
    LargeMessageBufferPool pool = new LargeMessageBufferPool(30, 20, false);
    LargeMessageSegment segment = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId, 2, 3, 25, 30);
    try {
      pool.tryCompleteMessage(tp, 0, segment);
      fail("Should throw large message exception for wrong segment size.");
    } catch (InvalidSegmentException ise) {
      assertTrue(ise.getMessage().startsWith("Segment size should not be larger"));
    }
  }

  @Test(expectedExceptions = InvalidSegmentException.class)
  public void testOutOfOrderSegment() {
    UUID messageId = LiKafkaClientsUtils.randomUUID();
    TopicPartition tp = new TopicPartition("topic", 0);
    LargeMessageBufferPool pool = new LargeMessageBufferPool(30, 20, false);
    LargeMessageSegment segment = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId, 2, 3, 25, 10);
    pool.tryCompleteMessage(tp, 1, segment);
    pool.tryCompleteMessage(tp, 0, segment);
  }

  @Test
  public void testClear() {
    LargeMessageBufferPool pool = new LargeMessageBufferPool(100, 20, true);

    TopicPartition tp0 = new TopicPartition("topic", 0);
    TopicPartition tp1 = new TopicPartition("topic", 1);
    UUID messageId0 = LiKafkaClientsUtils.randomUUID();
    UUID messageId1 = LiKafkaClientsUtils.randomUUID();

    long offset = 0;
    LargeMessageSegment m0Seg0 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 0, 3, 25, 10);
    LargeMessageSegment m0Seg1 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 1, 3, 25, 10);
    LargeMessageSegment m0Seg2 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId0, 2, 3, 25, 5);

    LargeMessageSegment m1Seg0 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId1, 0, 3, 25, 10);
    LargeMessageSegment m1Seg1 = LiKafkaClientsTestUtils.createLargeMessageSegment(messageId1, 1, 3, 25, 10);

    pool.tryCompleteMessage(tp0, offset++, m0Seg0);
    pool.tryCompleteMessage(tp0, offset++, m0Seg1);

    pool.tryCompleteMessage(tp1, offset++, m1Seg0);
    pool.tryCompleteMessage(tp1, offset++, m1Seg1);

    pool.clear(tp0);

    assertEquals(pool.size(), 1, "Buffer pool size should be 1.");
    assertEquals(pool.bufferUsed(), 20, "Buffer pool buffered bytes should be 0.");
    assertEquals(pool.safeOffsets().size(), 1, "Safe offset map size should only contain partition 0.");
    assertEquals(pool.safeOffsets().get(tp0), null, "Safe offset for partition 0 should not exist.");
    assertEquals(pool.safeOffsets().get(tp1).longValue(), 2, "Safe offset for partition 1 should be 2.");

    assertEquals(pool.tryCompleteMessage(tp0, offset, m0Seg2).serializedMessage(), null, "No message should be completed");
    assertEquals(pool.size(), 2, "Buffer pool size should be 1.");
    assertEquals(pool.bufferUsed(), 25, "Buffer pool buffered bytes should be 10.");
    assertEquals(pool.safeOffsets().size(), 2, "Safe offset map size should 1.");
    assertEquals(pool.safeOffsets().get(tp0).longValue(), 4, "Safe offset for partition 0 should be 0.");

    pool.clear();
    assertEquals(pool.size(), 0, "Buffer pool size should be 0.");
    assertEquals(pool.bufferUsed(), 0, "Buffer pool buffered bytes should be 0.");
    assertEquals(pool.safeOffsets().size(), 0, "Safe offset map size should only contain partition 0.");
    assertEquals(pool.safeOffsets().get(tp0), null, "Safe offset for partition 0 should not exist.");
    assertEquals(pool.safeOffsets().get(tp1), null, "Safe offset for partition 1 should not exist.");

  }


}
