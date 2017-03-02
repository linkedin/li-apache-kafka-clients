/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.headers.HeaderUtils;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Unit test for message assembler.
 */
public class MessageAssemblerTest {
  @Test
  public void testSingleMessageSegment() {
    // Create serializer/deserializers.
    byte[] messageBytes = "message".getBytes();

    LargeMessageSegment largeMessageSegment = new LargeMessageSegment(UUID.randomUUID(), 0, 1, messageBytes.length, true, ByteBuffer.wrap(messageBytes));
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord =
        new ExtensibleConsumerRecord<byte[], byte[]>("topic", 0, 0, 0, null, 0, 0, 0, "key".getBytes(), messageBytes);
    consumerRecord.header(HeaderUtils.LARGE_MESSAGE_SEGMENT_HEADER, largeMessageSegment.segmentHeader());
    MessageAssembler messageAssembler = new MessageAssemblerImpl(100, 100, true);
    MessageAssembler.AssembleResult assembleResult =
        messageAssembler.assemble(new TopicPartition("topic", 0), 0, consumerRecord);

    assertNotNull(assembleResult.messageBytes());
    assertEquals(assembleResult.messageStartingOffset(), 0, "The message starting offset should be 0");
    assertEquals(assembleResult.messageEndingOffset(), 0, "The message ending offset should be 0");
  }

}
