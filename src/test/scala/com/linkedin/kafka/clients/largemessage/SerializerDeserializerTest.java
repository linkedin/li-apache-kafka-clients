/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

    String s = TestUtils.getRandomString(100);
    assertEquals(s.length(), 100);
    byte[] stringBytes = stringSerializer.serialize("topic", s);
    assertEquals(stringBytes.length, 100);
    LargeMessageSegment segment =
        new LargeMessageSegment(UUID.randomUUID(), 0, 2, stringBytes.length, false, ByteBuffer.wrap(stringBytes));

    LargeMessageSegment deserializedSegment = new LargeMessageSegment(segment.segmentHeader(), segment.segmentByteBuffer());
    assertEquals(deserializedSegment.messageId(), segment.messageId());
    assertEquals(deserializedSegment.originalValueSize(), stringBytes.length);
    assertEquals(deserializedSegment.numberOfSegments(), 2);
    assertEquals(deserializedSegment.sequenceNumber(), 0);
    String deserializedString = stringDeserializer.deserialize("topic", deserializedSegment.segmentArray());
    assertEquals(deserializedString, s);

  }
}
