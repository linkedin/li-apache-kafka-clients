/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.largemessage.errors.InvalidSegmentException;
import com.linkedin.kafka.clients.utils.TestUtils;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Unit test for incomplete message.
 */
public class LargeMessageTest {
  private final int messageSizeInBytes = 15;
  private final int numberOfSegments = 2;
  private final UUID messageId = UUID.randomUUID();

  @Test
  public void testIncompleteMessage() {
    LargeMessage message = new LargeMessage(new TopicPartition("topic", 0),
        UUID.randomUUID(),
        0L,
        messageSizeInBytes,
        numberOfSegments);

    LargeMessageSegment segment0 = TestUtils.createLargeMessageSegment(messageId, 0, numberOfSegments, messageSizeInBytes, 10);
    LargeMessageSegment segment1 = TestUtils.createLargeMessageSegment(messageId, 1, numberOfSegments, messageSizeInBytes, 5);

    byte[] serializedMessage = message.addSegment(segment1, 1).serializedMessage();
    assert serializedMessage == null;

    assertEquals(message.bufferedSizeInBytes(), 5, "5 bytes should be buffered");
    serializedMessage = message.addSegment(segment0, 1).serializedMessage();
    assert serializedMessage != null;
    assertEquals(message.bufferedSizeInBytes(), 15, "15 bytes should be buffered");
    assert serializedMessage.length == messageSizeInBytes;

    // verify the bytes.
    TestUtils.verifyMessage(serializedMessage, messageSizeInBytes, 10);
  }

  @Test(expectedExceptions = InvalidSegmentException.class)
  public void testZeroLengthSegment() {
    LargeMessage message = new LargeMessage(new TopicPartition("topic", 0),
        UUID.randomUUID(),
        0L,
        messageSizeInBytes,
        numberOfSegments);

    LargeMessageSegment zeroLengthSegment = TestUtils.createLargeMessageSegment(messageId, 0, numberOfSegments, messageSizeInBytes, 0);
    message.addSegment(zeroLengthSegment, 0);
  }

  @Test(expectedExceptions = InvalidSegmentException.class)
  public void testSegmentTotalSizeGreaterThanMesssageSize() {
    LargeMessage message = new LargeMessage(new TopicPartition("topic", 0),
        UUID.randomUUID(),
        0L,
        messageSizeInBytes,
        numberOfSegments);

    LargeMessageSegment segment0 = TestUtils.createLargeMessageSegment(messageId, 0, numberOfSegments, messageSizeInBytes, 10);
    LargeMessageSegment segment1 = TestUtils.createLargeMessageSegment(messageId, 1, numberOfSegments, messageSizeInBytes, 10);
    message.addSegment(segment0, 0);
    message.addSegment(segment1, 1);
  }

  @Test
  public void testValidation() {
    LargeMessage message = new LargeMessage(new TopicPartition("topic", 0),
        UUID.randomUUID(),
        0L,
        messageSizeInBytes,
        numberOfSegments);

    LargeMessageSegment segment = TestUtils.createLargeMessageSegment(messageId, 0, numberOfSegments, messageSizeInBytes, 10);
    segment = TestUtils.createLargeMessageSegment(messageId, 0, numberOfSegments + 1, messageSizeInBytes, 10);
    try {
      message.addSegment(segment, numberOfSegments + 1);
      fail("Should throw exception.");
    } catch (Throwable t) {
      // too many segments
      assertTrue(t.getMessage().startsWith("Segment number of offsets"));
    }

    segment = TestUtils.createLargeMessageSegment(messageId, 0, numberOfSegments, messageSizeInBytes + 1, 10);
    try {
      message.addSegment(segment, numberOfSegments);
      fail("Should throw exception.");
    } catch (Throwable t) {
      // Bad original value size
      assertTrue(t.getMessage().startsWith("Segment number of offsets"));
    }
  }
}
