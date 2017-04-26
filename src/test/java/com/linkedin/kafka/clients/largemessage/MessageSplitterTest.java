/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.producer.UUIDFactory;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.TestUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * Unit test for message splitter.
 */
public class MessageSplitterTest {
  @Test
  public void testSplit() {
    TopicPartition tp = new TopicPartition("topic", 0);
    UUID id = LiKafkaClientsUtils.randomUUID();
    String message = TestUtils.getRandomString(1000);
    Serializer<String> stringSerializer = new StringSerializer();
    Deserializer<String> stringDeserializer = new StringDeserializer();
    Serializer<LargeMessageSegment> segmentSerializer = new DefaultSegmentSerializer();
    Deserializer<LargeMessageSegment> segmentDeserializer = new DefaultSegmentDeserializer();
    MessageSplitter splitter = new MessageSplitterImpl(200, segmentSerializer, new UUIDFactory.DefaultUUIDFactory<>());

    byte[] serializedMessage = stringSerializer.serialize("topic", message);
    List<ProducerRecord<byte[], byte[]>> records = splitter.split("topic", id, serializedMessage);
    assertEquals(records.size(), 5, "Should have 6 segments.");
    MessageAssembler assembler = new MessageAssemblerImpl(10000, 10000, true, segmentDeserializer);
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
