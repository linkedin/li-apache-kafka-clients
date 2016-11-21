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

import com.linkedin.kafka.clients.producer.ExtensibleProducerRecord;
import com.linkedin.kafka.clients.utils.TestUtils;
import com.linkedin.kafka.clients.utils.UUIDFactoryImpl;
import java.util.Collection;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * Unit test for message splitter.
 */
public class MessageSplitterTest {
  @Test
  public void testSplit() {
    TopicPartition tp = new TopicPartition("topic", 0);
    UUID id = UUID.randomUUID();
    String message = TestUtils.getRandomString(1000);
    Serializer<String> stringSerializer = new StringSerializer();
    Deserializer<String> stringDeserializer = new StringDeserializer();
    MessageSplitter splitter = new MessageSplitterImpl(200, new UUIDFactoryImpl(), null);

    byte[] serializedMessage = stringSerializer.serialize("topic", message);
    ExtensibleProducerRecord<byte[], byte[]> producerRecord =
        new ExtensibleProducerRecord<byte[], byte[]>("topic", 0, null, "key".getBytes(), serializedMessage);
    Collection<ExtensibleProducerRecord<byte[], byte[]>> records = splitter.split(producerRecord);
    assertEquals(records.size(), 5, "Should have 6 segments.");
    MessageAssembler assembler = new MessageAssemblerImpl(10000, 10000, true);
    String assembledMessage = null;
    UUID uuid = null;
    for (int i = 0; i < records.size(); i++) {
      ProducerRecord<byte[], byte[]> record = records.get(i);
      LargeMessageSegment segment = segmentDeserializer.deserialize("topic", record.value());
      if (uuid == null) {
        uuid = segment.messageId;
      } else {
        assertEquals(segment.messageId, uuid, "messageId should match.");
      }
      assertEquals(segment.numberOfSegments, 5, "segment number should be 5");
      assertEquals(segment.messageSizeInBytes, serializedMessage.length, "message size should the same");
      assertEquals(segment.sequenceNumber, i, "SequenceNumber should match");

      assembledMessage = stringDeserializer.deserialize(null, assembler.assemble(tp, i, record.value()).messageBytes());
    }
    assertEquals(assembledMessage, message, "messages should match.");
  }

}
