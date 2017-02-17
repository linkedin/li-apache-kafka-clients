/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.utils.TestUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * Test for DefaultSegmentSerializer and DefaultSegmentDeserializer.
 */
public class SerializerDeserializerTest {

  @Test
  public void testSerde() {
    Serializer<String> stringSerializer = new StringSerializer();
    Deserializer<String> stringDeserializer = new StringDeserializer();
    Serializer<LargeMessageSegment> segmentSerializer = new DefaultSegmentSerializer();
    Deserializer<LargeMessageSegment> segmentDeserializer = new DefaultSegmentDeserializer();

    String s = TestUtils.getRandomString(100);
    assertEquals(s.length(), 100);
    byte[] stringBytes = stringSerializer.serialize("topic", s);
    assertEquals(stringBytes.length, 100);
    LargeMessageSegment segment =
        new LargeMessageSegment(UUID.randomUUID(), 0, 2, stringBytes.length, ByteBuffer.wrap(stringBytes));
    // String bytes + segment header
    byte[] serializedSegment = segmentSerializer.serialize("topic", segment);
    assertEquals(serializedSegment.length, 1 + stringBytes.length + LargeMessageSegment.SEGMENT_INFO_OVERHEAD + 4);

    LargeMessageSegment deserializedSegment = segmentDeserializer.deserialize("topic", serializedSegment);
    assertEquals(deserializedSegment.messageId, segment.messageId);
    assertEquals(deserializedSegment.messageSizeInBytes, segment.messageSizeInBytes);
    assertEquals(deserializedSegment.numberOfSegments, segment.numberOfSegments);
    assertEquals(deserializedSegment.sequenceNumber, segment.sequenceNumber);
    assertEquals(deserializedSegment.payload.limit(), 100);
    String deserializedString = stringDeserializer.deserialize("topic", deserializedSegment.payloadArray());
    assertEquals(deserializedString.length(), s.length());

  }
}
