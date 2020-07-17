/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.common.LargeMessageHeaderValue;
import com.linkedin.kafka.clients.utils.Constants;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.largemessage.errors.InvalidSegmentException;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
  public void testSingleMessageSegmentWithRecordHeader() {
    byte[] messageBytes = "message".getBytes();
    // create record header
    Headers headers = new RecordHeaders();
    LargeMessageHeaderValue largeMessageHeaderValue =
        new LargeMessageHeaderValue(LargeMessageHeaderValue.V3, LiKafkaClientsUtils.randomUUID(), 0, 1,
            messageBytes.length);
    headers.add(Constants.LARGE_MESSAGE_HEADER, LargeMessageHeaderValue.toBytes(largeMessageHeaderValue));

    MessageAssembler messageAssembler = new MessageAssemblerImpl(100, 100, true);

    MessageAssembler.AssembleResult assembleResult =
        messageAssembler.assemble(new TopicPartition("topic", 0), 0, messageBytes,
            headers.lastHeader(Constants.LARGE_MESSAGE_HEADER));

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

  @Test(expectedExceptions = InvalidSegmentException.class)
  public void testBadSegmentInHeaderThrows() {
    byte[] messageBytes = "message".getBytes();
    // create record header
    Headers headers = new RecordHeaders();
    LargeMessageHeaderValue badLargeMessageHeaderValue =
        new LargeMessageHeaderValue(LargeMessageHeaderValue.V3, LiKafkaClientsUtils.randomUUID(), -1, 100, -1);
    headers.add(Constants.LARGE_MESSAGE_HEADER, LargeMessageHeaderValue.toBytes(badLargeMessageHeaderValue));

    MessageAssembler messageAssembler = new MessageAssemblerImpl(100, 100, true);
    TopicPartition tp = new TopicPartition("topic", 0);
    messageAssembler.assemble(tp, 0, messageBytes, headers.lastHeader(Constants.LARGE_MESSAGE_HEADER));
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

  @Test
  public void testNonLargeMessageSegmentBytesWithHeader() {
    Deserializer<LargeMessageSegment> segmentDeserializer = new DefaultSegmentDeserializer();

    MessageAssembler messageAssembler = new MessageAssemblerImpl(100, 100, true, segmentDeserializer);

    byte[] bytes = new byte[100];
    // record header is null in this case
    MessageAssembler.AssembleResult assembleResult =
        messageAssembler.assemble(new TopicPartition("topic", 0), 0, bytes, null);
    assertEquals(assembleResult.messageBytes(), bytes, "The bytes should be returned as is");
    assertEquals(assembleResult.messageStartingOffset(), 0, "The message starting offset should be 0");
    assertEquals(assembleResult.messageEndingOffset(), 0, "The message ending offset should be 0");
  }

  private byte[] wrapMessageBytes(Serializer<LargeMessageSegment> segmentSerializer, byte[] messageBytes) {
    return segmentSerializer.serialize("topic",
        new LargeMessageSegment(LiKafkaClientsUtils.randomUUID(), 0, 1, messageBytes.length, ByteBuffer.wrap(messageBytes)));
  }
}
