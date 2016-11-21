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

import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.consumer.HeaderKeySpace;
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
    consumerRecord.header(HeaderKeySpace.LARGE_MESSAGE_SEGMENT_HEADER, largeMessageSegment.segmentHeader());
    MessageAssembler messageAssembler = new MessageAssemblerImpl(100, 100, true);
    MessageAssembler.AssembleResult assembleResult =
        messageAssembler.assemble(new TopicPartition("topic", 0), 0, consumerRecord);

    assertNotNull(assembleResult.messageBytes());
    assertEquals(assembleResult.messageStartingOffset(), 0, "The message starting offset should be 0");
    assertEquals(assembleResult.messageEndingOffset(), 0, "The message ending offset should be 0");
  }

}
