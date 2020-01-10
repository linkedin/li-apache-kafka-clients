/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DefaultSegmentDeserializerTest {

  @Test
  public void testZeroChecksum() {
    DefaultSegmentSerializer segmentSerializer = new DefaultSegmentSerializer();

    //doctor a UUID such that the projected checksum is 0
    long a = (Long.MAX_VALUE / 2) - 1;
    long b = (Long.MAX_VALUE / 2) + 3;
    int checksum = (int) (a + b);

    Assert.assertEquals(checksum, 0, "projected checksum should be 0. instead was " + checksum);

    UUID msgId = new UUID(a, b);
    byte[] payload = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    LargeMessageSegment segment = new LargeMessageSegment(msgId, 0, 1, 10, ByteBuffer.wrap(payload));

    byte[] serialized = segmentSerializer.serialize("topic", segment);

    DefaultSegmentDeserializer segmentDeserializer = new DefaultSegmentDeserializer();

    LargeMessageSegment deserialized = segmentDeserializer.deserialize("topic", serialized);

    Assert.assertNotNull(deserialized);
    Assert.assertEquals(deserialized.messageId, msgId);
    Assert.assertTrue(Arrays.equals(payload, deserialized.payloadArray()));
  }
}
