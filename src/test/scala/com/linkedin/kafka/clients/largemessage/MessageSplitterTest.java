/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.consumer.HeaderKeySpace;
import com.linkedin.kafka.clients.producer.ExtensibleProducerRecord;
import com.linkedin.kafka.clients.utils.TestUtils;
import com.linkedin.kafka.clients.utils.UUIDFactory;
import com.linkedin.kafka.clients.utils.UUIDFactoryImpl;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit test for message splitter.
 */
public class MessageSplitterTest {
  @Test
  public void testSplit() {
    TopicPartition tp = new TopicPartition("topic", 0);
    UUID id = UUID.randomUUID();
    UUIDFactory uuidFactory = new UUIDFactoryImpl() {
      @Override
      public UUID create() {
        return id;
      }
    };
    String message = TestUtils.getRandomString(1000);
    Serializer<String> stringSerializer = new StringSerializer();
    Deserializer<String> stringDeserializer = new StringDeserializer();
    MessageSplitter splitter = new MessageSplitterImpl(200, uuidFactory, null);

    byte[] serializedMessage = stringSerializer.serialize("topic", message);
    ExtensibleProducerRecord<byte[], byte[]> producerRecord =
        new ExtensibleProducerRecord<>("topic", 0, null, "key".getBytes(), serializedMessage);
    Collection<ExtensibleProducerRecord<byte[], byte[]>> records = splitter.split(producerRecord);
    assertEquals(records.size(), 5, "Should have 5 segments.");
    MessageAssembler assembler = new MessageAssemblerImpl(10000, 10000, true);

    int expectedSequenceNumber = 0;
    MessageAssembler.AssembleResult assembledMessage = null;
    int totalHeadersSize = 0;

    for (ExtensibleProducerRecord<byte[], byte[]> splitRecord : records) {
      ExtensibleConsumerRecord<byte[], byte[]> splitConsumerRecord = TestUtils.producerRecordToConsumerRecord(splitRecord,
          expectedSequenceNumber, expectedSequenceNumber, null, 0, 0);
      totalHeadersSize += splitRecord.header(HeaderKeySpace.LARGE_MESSAGE_SEGMENT_HEADER).length + 4 + 4;
      assembledMessage = assembler.assemble(tp, expectedSequenceNumber, splitConsumerRecord);
      if (expectedSequenceNumber != 4) {
        assertNull(assembledMessage.messageBytes());
      }

      // Check that each segment looks good
      LargeMessageSegment segment =
          new LargeMessageSegment(splitConsumerRecord.header(HeaderKeySpace.LARGE_MESSAGE_SEGMENT_HEADER), ByteBuffer.wrap(splitConsumerRecord.value()));
      assertEquals(segment.messageId(), id, "messageId should match.");
      assertEquals(segment.numberOfSegments(), 5, "number of segments should be 5");
      assertEquals(segment.originalValueSize(), serializedMessage.length, "original value size should the same");
      assertEquals(segment.sequenceNumber(), expectedSequenceNumber, "SequenceNumber should match");
      assertEquals(segment.originalKeyWasNull(), false);
      expectedSequenceNumber++;
    }

    assertTrue(totalHeadersSize != 0);
    assertEquals(assembledMessage.totalHeadersSize(), totalHeadersSize);
    String deserializedOriginalValue = stringDeserializer.deserialize(null, assembledMessage.messageBytes());
    assertEquals(deserializedOriginalValue, message, "values should match.");
  }

}
