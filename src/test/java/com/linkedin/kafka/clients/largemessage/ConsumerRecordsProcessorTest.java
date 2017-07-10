/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.largemessage.errors.OffsetNotTrackedException;
import com.linkedin.kafka.clients.largemessage.errors.SkippableException;
import com.linkedin.kafka.clients.producer.UUIDFactory;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.LiKafkaClientsTestUtils;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.*;


/**
 * Unit test for consumer record filter.
 */
public class ConsumerRecordsProcessorTest {

  @Test
  public void testFilter() throws Exception {
    // Create consumer record processor
    Serializer<String> stringSerializer = new StringSerializer();
    Serializer<LargeMessageSegment> segmentSerializer = new DefaultSegmentSerializer();
    ConsumerRecordsProcessor<String, String> consumerRecordsProcessor = createConsumerRecordsProcessor();

    // Let consumer record 0 be a normal record.
    String message0 = "message0";
    ConsumerRecord<byte[], byte[]> consumerRecord0 =
        new ConsumerRecord<>("topic", 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(),
                             stringSerializer.serialize("topic", message0));

    // Let consumer record 1 be a large message.
    byte[] message1Bytes =
        segmentSerializer.serialize("topic",
                                    LiKafkaClientsTestUtils.createLargeMessageSegment(LiKafkaClientsUtils.randomUUID(), 0, 2, 20, 10));
    ConsumerRecord<byte[], byte[]> consumerRecord1 =
        new ConsumerRecord<>("topic", 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), message1Bytes);

    // Construct the consumer records.
    List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    recordList.add(consumerRecord0);
    recordList.add(consumerRecord1);
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
    recordsMap.put(new TopicPartition("topic", 0), recordList);
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(recordsMap);

    ConsumerRecords<String, String> filteredRecords = consumerRecordsProcessor.process(records).consumerRecords();
    ConsumerRecord<String, String> consumerRecord = filteredRecords.iterator().next();
    assertEquals(filteredRecords.count(), 1, "Only one record should be there after filtering.");
    assertEquals(consumerRecord0.topic(), consumerRecord.topic(), "Topic should match");
    assertEquals(consumerRecord0.partition(), consumerRecord.partition(), "partition should match");
    assertTrue(Arrays.equals(consumerRecord0.key(), consumerRecord.key().getBytes()), "key should match");
    assertEquals(consumerRecord0.offset(), consumerRecord.offset(), "Offset should match");
    assertEquals(consumerRecord.value(), "message0", "\"message0\" should be the value");
  }

  @Test
  public void testCorrectness() {
    ConsumerRecordsProcessor<String, String> consumerRecordsProcessor = createConsumerRecordsProcessor();
    ConsumerRecords<String, String> processedRecords = consumerRecordsProcessor.process(getConsumerRecords()).consumerRecords();
    assertEquals(processedRecords.count(), 4, "There should be 4 records");
    Iterator<ConsumerRecord<String, String>> iter = processedRecords.iterator();
    assertEquals(iter.next().offset(), 0, "Message offset should b 0");
    assertEquals(iter.next().offset(), 2, "Message offset should b 2");
    assertEquals(iter.next().offset(), 4, "Message offset should b 4");
    assertEquals(iter.next().offset(), 5, "Message offset should b 5");
  }

  @Test
  public void testSafeOffsetWithoutLargeMessage() throws IOException {
    Serializer<String> stringSerializer = new StringSerializer();
    Serializer<LargeMessageSegment> segmentSerializer = new DefaultSegmentSerializer();
    ConsumerRecordsProcessor<String, String> consumerRecordsProcessor = createConsumerRecordsProcessor();

    // Let consumer record 0 and 1 be a normal record.
    // Let consumer record 0 be a normal record.
    byte[] message0Bytes = stringSerializer.serialize("topic", "message0");
    byte[] message0WrappedBytes = wrapMessageBytes(segmentSerializer, message0Bytes);
    ConsumerRecord<byte[], byte[]> consumerRecord0 =
        new ConsumerRecord<>("topic", 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), message0WrappedBytes);

    // Let consumer record 1 be a normal message.
    byte[] message1Bytes = stringSerializer.serialize("topic", "message1");
    byte[] message1WrappedBytes = wrapMessageBytes(segmentSerializer, message1Bytes);
    ConsumerRecord<byte[], byte[]> consumerRecord1 =
        new ConsumerRecord<>("topic", 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), message1WrappedBytes);

    // Construct the consumer records.
    TopicPartition tp = new TopicPartition("topic", 0);
    List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    recordList.add(consumerRecord0);
    recordList.add(consumerRecord1);
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap =
        new HashMap<>();
    recordsMap.put(tp, recordList);
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(recordsMap);

    consumerRecordsProcessor.process(records).consumerRecords();
    Map<TopicPartition, OffsetAndMetadata> safeOffsets = consumerRecordsProcessor.safeOffsetsToCommit();
    assertEquals(safeOffsets.size(), 1, "Safe offsets should contain one entry");
    assertEquals(safeOffsets.get(tp).offset(), 2, "Safe offset of topic partition 0 should be 2");
    assertEquals(consumerRecordsProcessor.safeOffset(tp, 0L).longValue(), 1, "safe offset should be 1");
    assertEquals(consumerRecordsProcessor.safeOffset(tp, 1L).longValue(), 2, "safe offset should be 2");

    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(tp, new OffsetAndMetadata(1L));
    safeOffsets = consumerRecordsProcessor.safeOffsetsToCommit(offsetMap, false);
    assertEquals(safeOffsets.get(tp).offset(), 1L, "Safe offset of topic partition 0 should be 1");

    offsetMap.put(tp, new OffsetAndMetadata(2L));
    safeOffsets = consumerRecordsProcessor.safeOffsetsToCommit(offsetMap, false);
    assertEquals(safeOffsets.get(tp).offset(), 2L, "Safe offset of topic partition 0 should be 2");
  }

  @Test
  public void testSafeOffsetWithLargeMessage() throws IOException {
    ConsumerRecordsProcessor<String, String> consumerRecordsProcessor = createConsumerRecordsProcessor();
    consumerRecordsProcessor.process(getConsumerRecords()).consumerRecords();

    // check safe offsets
    TopicPartition tp = new TopicPartition("topic", 0);
    Map<TopicPartition, OffsetAndMetadata> safeOffsets = consumerRecordsProcessor.safeOffsetsToCommit();
    assertEquals(safeOffsets.size(), 1, "Safe offsets map should contain 1 entry");
    assertEquals(consumerRecordsProcessor.safeOffset(tp, 0L).longValue(), 1, "safe offset should be 1");
    try {
      consumerRecordsProcessor.safeOffset(tp, 1L);
      fail("Should throw exception because offset is invalid.");
    } catch (OffsetNotTrackedException onte) {
      assertTrue(onte.getMessage().startsWith("Offset 1 for partition"));
    }
    assertEquals(consumerRecordsProcessor.safeOffset(tp, 2L).longValue(), 1, "safe offset should be 1");
    try {
      consumerRecordsProcessor.safeOffset(tp, 3L);
      fail("Should throw exception because offset is invalid.");
    } catch (OffsetNotTrackedException onte) {
      assertTrue(onte.getMessage().startsWith("Offset 3 for partition"));
    }
    assertEquals(consumerRecordsProcessor.safeOffset(tp, 4L).longValue(), 1, "safe offset should be 1");
    assertEquals(consumerRecordsProcessor.safeOffset(tp, 5L).longValue(), 6, "safe offset should be 6");
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 4L), 3, "Starting offset of large message 2 should be 3");
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 5L), 1, "Starting offset of large message 1 should be 1");
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 0L), 0, "Starting offset of large message 0 should be 0");
  }

  @Test
  public void testEviction() {
    Serializer<String> stringSerializer = new StringSerializer();
    Serializer<LargeMessageSegment> segmentSerializer = new DefaultSegmentSerializer();
    // Create two large messages.
    MessageSplitter splitter = new MessageSplitterImpl(500, segmentSerializer, new UUIDFactory.DefaultUUIDFactory<>());

    ConsumerRecordsProcessor<String, String> consumerRecordsProcessor = createConsumerRecordsProcessor();
    consumerRecordsProcessor.process(getConsumerRecords()).consumerRecords();
    // The offset tracker now has 2, 4, 5 in it.
    TopicPartition tp = new TopicPartition("topic", 0);

    UUID largeMessageId = LiKafkaClientsUtils.randomUUID();
    byte[] largeMessage1Bytes = stringSerializer.serialize("topic", LiKafkaClientsTestUtils.getRandomString(600));
    List<ProducerRecord<byte[], byte[]>> splitLargeMessage =
        splitter.split("topic", largeMessageId, largeMessage1Bytes);

    // Test evict
    List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<ConsumerRecord<byte[], byte[]>>();
    // Let consumer record 6 be a large message segment.
    ConsumerRecord<byte[], byte[]> consumerRecord6 =
        new ConsumerRecord<>("topic", 0, 6, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), splitLargeMessage.get(0).value());
    // Let consumer record 7 be a normal record.
    ConsumerRecord<byte[], byte[]> consumerRecord7 =
        new ConsumerRecord<>("topic", 0, 7, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(),
                             stringSerializer.serialize("topic", "message7"));
    // Let consumer record 8 completes consumer record 6
    ConsumerRecord<byte[], byte[]> consumerRecord8 =
        new ConsumerRecord<>("topic", 0, 8, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), splitLargeMessage.get(1).value());

    recordList.add(consumerRecord6);
    recordList.add(consumerRecord7);
    recordList.add(consumerRecord8);

    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
    recordsMap.put(new TopicPartition("topic", 0), recordList);
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(recordsMap);
    consumerRecordsProcessor.process(records).consumerRecords();
    // Now the offset tracker should have 4, 5, 6, 8 in side it.
    assertEquals(consumerRecordsProcessor.safeOffset(tp, 7L).longValue(), 6, "safe offset should be 6");

    try {
      consumerRecordsProcessor.safeOffset(tp, 2L);
      fail("Should throw exception because offset for message 2 should have been evicted.");
    } catch (OffsetNotTrackedException onte) {
      assertTrue(onte.getMessage().startsWith("Offset 2 for partition"));
    }
  }

  @Test
  public void verifyStartingOffset() {
    ConsumerRecordsProcessor<String, String> consumerRecordsProcessor = createConsumerRecordsProcessor();
    consumerRecordsProcessor.process(getConsumerRecords()).consumerRecords();

    TopicPartition tp = new TopicPartition("topic", 0);
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 4L), 3, "Starting offset of large message 2 should be 3");
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 5L), 1, "Starting offset of large message 1 should be 1");
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 0L), 0, "Starting offset of large message 0 should be 0");
  }

  @Test
  public void testStartingOffsetWithoutMessages() throws IOException {
    ConsumerRecordsProcessor<String, String> consumerRecordsProcessor = createConsumerRecordsProcessor();

    TopicPartition tp = new TopicPartition("topic", 0);
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 100L), 100, "Should return 100 because there are no " +
        "large messages in the partition.");
  }

  @Test(expectedExceptions = OffsetNotTrackedException.class)
  public void testStartingOffsetWithNormalMessages() throws IOException {
    Serializer<String> stringSerializer = new StringSerializer();
    Serializer<LargeMessageSegment> segmentSerializer = new DefaultSegmentSerializer();
    ConsumerRecordsProcessor<String, String> consumerRecordsProcessor = createConsumerRecordsProcessor();

    // Let consumer record 0 be a normal record.
    byte[] message0Bytes = stringSerializer.serialize("topic", "message0");
    byte[] message0WrappedBytes = wrapMessageBytes(segmentSerializer, message0Bytes);
    ConsumerRecord<byte[], byte[]> consumerRecord0 =
        new ConsumerRecord<>("topic", 0, 100L, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), message0WrappedBytes);

    // Construct the consumer records.
    List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    recordList.add(consumerRecord0);
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
    recordsMap.put(new TopicPartition("topic", 0), recordList);
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(recordsMap);

    consumerRecordsProcessor.process(records).consumerRecords();

    TopicPartition tp = new TopicPartition("topic", 0);
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 100L), 100, "Should return 100 because there are no " +
        "large messages in the partition.");

    // Should throw exception when an offset cannot be found by the offset tracker.
    consumerRecordsProcessor.startingOffset(tp, 0L);
  }

  @Test
  public void testLastDelivered() {
    ConsumerRecordsProcessor<String, String> consumerRecordsProcessor = createConsumerRecordsProcessor();
    consumerRecordsProcessor.process(getConsumerRecords()).consumerRecords();

    assertEquals(consumerRecordsProcessor.delivered(new TopicPartition("topic", 0)).longValue(), 5L,
                 "The last deivered message should be 5");

    assertNull(consumerRecordsProcessor.delivered(new TopicPartition("topic", 1)));
  }

  @Test
  public void testNullValue() {
    ConsumerRecordsProcessor<String, String> consumerRecordsProcessor = createConsumerRecordsProcessor();
    ConsumerRecord<byte[], byte[]> consumerRecord =
        new ConsumerRecord<>("topic", 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), null);
    ConsumerRecords<byte[], byte[]> consumerRecords =
        new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("topic", 0), Collections.singletonList(consumerRecord)));
    ConsumerRecords<String, String> processedRecords = consumerRecordsProcessor.process(consumerRecords).consumerRecords();
    assertNull(processedRecords.iterator().next().value());
  }

  @Test
  public void testDeserializationException() {
    TopicPartition tp0 = new TopicPartition("topic", 0);
    TopicPartition tp1 = new TopicPartition("topic", 1);
    TopicPartition tp2 = new TopicPartition("topic", 2);
    Deserializer<String> stringDeserializer = new StringDeserializer();
    Deserializer<String> errorThrowingDeserializer = new Deserializer<String>() {
      @Override
      public void configure(Map<String, ?> configs, boolean isKey) {

      }

      @Override
      public String deserialize(String topic, byte[] data) {
        String s = stringDeserializer.deserialize(topic, data);
        if (s.equals("ErrorBytes")) {
          throw new SkippableException();
        }
        return s;
      }

      @Override
      public void close() {

      }
    };
    Deserializer<LargeMessageSegment> segmentDeserializer = new DefaultSegmentDeserializer();
    MessageAssembler assembler = new MessageAssemblerImpl(5000, 100, false, segmentDeserializer);
    DeliveredMessageOffsetTracker deliveredMessageOffsetTracker = new DeliveredMessageOffsetTracker(4);
    ConsumerRecordsProcessor processor0 =  new ConsumerRecordsProcessor<>(assembler, stringDeserializer, errorThrowingDeserializer,
                                                                          deliveredMessageOffsetTracker, null, false);
    ConsumerRecordsProcessor processor1 =  new ConsumerRecordsProcessor<>(assembler, stringDeserializer, errorThrowingDeserializer,
                                                                          deliveredMessageOffsetTracker, null, true);

    StringSerializer stringSerializer = new StringSerializer();
    ConsumerRecord<byte[], byte[]> consumerRecord0 = new ConsumerRecord<>("topic", 0, 0, null,
                                                                          stringSerializer.serialize("topic", "value"));
    ConsumerRecord<byte[], byte[]> consumerRecord1 = new ConsumerRecord<>("topic", 0, 1, null,
                                                                          stringSerializer.serialize("topic", "ErrorBytes"));
    ConsumerRecord<byte[], byte[]> consumerRecord2 = new ConsumerRecord<>("topic", 0, 2, null,
                                                                          stringSerializer.serialize("topic", "value"));

    ConsumerRecord<byte[], byte[]> consumerRecord3 = new ConsumerRecord<>("topic", 1, 0, null,
                                                                          stringSerializer.serialize("topic", "ErrorBytes"));
    ConsumerRecord<byte[], byte[]> consumerRecord4 = new ConsumerRecord<>("topic", 1, 1, null,
                                                                          stringSerializer.serialize("topic", "value"));

    ConsumerRecord<byte[], byte[]> consumerRecord5 = new ConsumerRecord<>("topic", 2, 0, null,
                                                                          stringSerializer.serialize("topic", "value"));

    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordMap = new HashMap<>();
    recordMap.put(tp0, Arrays.asList(consumerRecord0, consumerRecord1, consumerRecord2));
    recordMap.put(tp1, Arrays.asList(consumerRecord3, consumerRecord4));
    recordMap.put(tp2, Collections.singletonList(consumerRecord5));

    ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(recordMap);

    // Process with skip record turned off.
    ConsumerRecordsProcessResult result = processor0.process(consumerRecords);
    assertEquals(result.consumerRecords().count(), 2);
    assertEquals(result.consumerRecords().records(tp0).size(), 1);
    assertTrue(result.consumerRecords().records(tp1).isEmpty());
    assertEquals(result.consumerRecords().records(tp2).size(), 1);
    assertEquals(result.resumeOffsets().get(tp0), 2L);
    assertEquals(result.resumeOffsets().get(tp1), 1L);
    assertNull(result.resumeOffsets().get(tp2));
    assertNotNull(result.exception());
    assertEquals(result.exception().recordProcessingExceptions().size(), 2);

    // process with skip record turned on
    result = processor1.process(consumerRecords);
    assertEquals(result.consumerRecords().count(), 4);
    assertEquals(result.consumerRecords().records(tp0).size(), 2);
    assertEquals(result.consumerRecords().records(tp1).size(), 1);
    assertEquals(result.consumerRecords().records(tp2).size(), 1);
    assertTrue(result.resumeOffsets().isEmpty());
    assertNull(result.exception());
  }

  private ConsumerRecords<byte[], byte[]> getConsumerRecords() {
    Serializer<String> stringSerializer = new StringSerializer();
    Serializer<LargeMessageSegment> segmentSerializer = new DefaultSegmentSerializer();
    // Create two large messages.
    MessageSplitter splitter = new MessageSplitterImpl(500, segmentSerializer, new UUIDFactory.DefaultUUIDFactory<>());

    UUID largeMessageId1 = LiKafkaClientsUtils.randomUUID();
    byte[] largeMessage1Bytes = stringSerializer.serialize("topic", LiKafkaClientsTestUtils.getRandomString(600));
    List<ProducerRecord<byte[], byte[]>> splitLargeMessage1 =
        splitter.split("topic", largeMessageId1, largeMessage1Bytes);

    UUID largeMessageId2 = LiKafkaClientsUtils.randomUUID();
    byte[] largeMessage2Bytes = stringSerializer.serialize("topic", LiKafkaClientsTestUtils.getRandomString(600));
    List<ProducerRecord<byte[], byte[]>> splitLargeMessage2 =
        splitter.split("topic", largeMessageId2, largeMessage2Bytes);

    // Let consumer record 0 be a normal record.
    ConsumerRecord<byte[], byte[]> consumerRecord0 =
        new ConsumerRecord<>("topic", 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), stringSerializer.serialize("topic", "message0"));
    // Let consumer record 1 be a large message segment
    ConsumerRecord<byte[], byte[]> consumerRecord1 =
        new ConsumerRecord<>("topic", 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), splitLargeMessage1.get(0).value());
    // Let consumer record 2 be a normal message
    ConsumerRecord<byte[], byte[]> consumerRecord2 =
        new ConsumerRecord<>("topic", 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), stringSerializer.serialize("topic", "message1"));
    // Let record 3 be a new large message segment
    ConsumerRecord<byte[], byte[]> consumerRecord3 =
        new ConsumerRecord<>("topic", 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), splitLargeMessage2.get(0).value());
    // let record 4 completes record 3
    ConsumerRecord<byte[], byte[]> consumerRecord4 =
        new ConsumerRecord<>("topic", 0, 4, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), splitLargeMessage2.get(1).value());
    // let record 5 completes record 1
    ConsumerRecord<byte[], byte[]> consumerRecord5 =
        new ConsumerRecord<>("topic", 0, 5, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), splitLargeMessage1.get(1).value());

    // Construct the consumer records.
    List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    recordList.add(consumerRecord0);
    recordList.add(consumerRecord1);
    recordList.add(consumerRecord2);
    recordList.add(consumerRecord3);
    recordList.add(consumerRecord4);
    recordList.add(consumerRecord5);
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap =
        new HashMap<>();
    recordsMap.put(new TopicPartition("topic", 0), recordList);
    return new ConsumerRecords<>(recordsMap);
  }

  private ConsumerRecordsProcessor<String, String> createConsumerRecordsProcessor() {
    Deserializer<String> stringDeserializer = new StringDeserializer();
    Deserializer<LargeMessageSegment> segmentDeserializer = new DefaultSegmentDeserializer();
    MessageAssembler assembler = new MessageAssemblerImpl(5000, 100, false, segmentDeserializer);
    DeliveredMessageOffsetTracker deliveredMessageOffsetTracker = new DeliveredMessageOffsetTracker(4);
    return new ConsumerRecordsProcessor<>(assembler, stringDeserializer, stringDeserializer,
                                          deliveredMessageOffsetTracker, null, false);
  }

  private byte[] wrapMessageBytes(Serializer<LargeMessageSegment> segmentSerializer, byte[] messageBytes) {
    return segmentSerializer.serialize("topic",
                                       new LargeMessageSegment(LiKafkaClientsUtils.randomUUID(), 0, 1, messageBytes.length,
                                                               ByteBuffer.wrap(messageBytes)));
  }
}
