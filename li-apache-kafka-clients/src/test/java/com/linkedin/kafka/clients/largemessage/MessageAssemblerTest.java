/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Unit test for message assembler.
 */
public class MessageAssemblerTest {

  @Test
  public void testSingleMessageSegment() {
    // Create serializer/deserializers.
    Serializer<LargeMessageSegment> segmentSerializer = new DefaultSegmentSerializer();
    Deserializer<LargeMessageSegment> segmentDeserializer = new DefaultSegmentDeserializer();

    byte[] messageWrappedBytes = wrapMessageBytes(segmentSerializer, "message".getBytes());

    MessageAssembler messageAssembler = new MessageAssemblerImpl(100, 100, true, segmentDeserializer);
    MessageAssembler.AssembleResult assembleResult =
        messageAssembler.assemble(new TopicPartition("topic", 0), 0, messageWrappedBytes);

    assertNotNull(assembleResult.messageBytes());
    assertEquals(assembleResult.messageStartingOffset(), 0, "The message starting offset should be 0");
    assertEquals(assembleResult.messageEndingOffset(), 0, "The message ending offset should be 0");
  }

  @Test
  public void testTreatBadSegmentAsPayload() {
    Serializer<LargeMessageSegment> segmentSerializer = new DefaultSegmentSerializer();
    Deserializer<LargeMessageSegment> segmentDeserializer = new DefaultSegmentDeserializer();
    MessageAssembler messageAssembler = new MessageAssemblerImpl(100, 100, true, segmentDeserializer);
    TopicPartition tp = new TopicPartition("topic", 0);

    UUID uuid = UUID.randomUUID();
    byte[] realPayload = "message".getBytes();
    LargeMessageSegment badSegment = new LargeMessageSegment(uuid, -1, 100, -1, ByteBuffer.wrap(realPayload));
    byte[] messageWrappedBytes = segmentSerializer.serialize(tp.topic(), badSegment);
    Assert.assertTrue(messageWrappedBytes.length > realPayload.length); //wrapping has been done

    messageAssembler.assemble(tp, 0, messageWrappedBytes);

    MessageAssembler.AssembleResult assembleResult = messageAssembler.assemble(tp, 0, messageWrappedBytes);
    Assert.assertEquals(assembleResult.messageBytes(), messageWrappedBytes);
    Assert.assertEquals(assembleResult.messageStartingOffset(), 0);
    Assert.assertEquals(assembleResult.messageEndingOffset(), 0);
  }

  @Test
  public void testNonLargeMessageSegmentBytes() {
    // Create serializer/deserializers.
    Deserializer<LargeMessageSegment> segmentDeserializer = new DefaultSegmentDeserializer();
    MessageAssembler messageAssembler = new MessageAssemblerImpl(100, 100, true, segmentDeserializer);

    byte[] bytes = new byte[100];
    MessageAssembler.AssembleResult assembleResult =
        messageAssembler.assemble(new TopicPartition("topic", 0), 0, bytes);
    assertEquals(assembleResult.messageBytes(), bytes, "The bytes should be returned as is");
    assertEquals(assembleResult.messageStartingOffset(), 0, "The message starting offset should be 0");
    assertEquals(assembleResult.messageEndingOffset(), 0, "The message ending offset should be 0");
  }

  private byte[] wrapMessageBytes(Serializer<LargeMessageSegment> segmentSerializer, byte[] messageBytes) {
    return segmentSerializer.serialize("topic",
        new LargeMessageSegment(LiKafkaClientsUtils.randomUUID(), 0, 1, messageBytes.length, ByteBuffer.wrap(messageBytes)));
  }
}
