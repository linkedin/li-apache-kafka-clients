/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.consumer.ConsumerRecordsProcessor;
import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.utils.HeaderKeySpace;
import com.linkedin.kafka.clients.largemessage.errors.OffsetNotTrackedException;
import com.linkedin.kafka.clients.producer.ExtensibleProducerRecord;
import com.linkedin.kafka.clients.utils.TestUtils;
import com.linkedin.kafka.clients.utils.UUIDFactoryImpl;
import java.util.Collection;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Unit test for consumer record filter.
 */
public class ConsumerRecordsProcessorTest {

  @Test
  public void testFilter() throws Exception {
    // Create consumer record processor
    Serializer<String> stringSerializer = new StringSerializer();
    ConsumerRecordsProcessor consumerRecordsProcessor = createConsumerRecordsProcessor();

    // Let consumer record 0 be a normal record.
    String message0 = "message0";
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord0 =
        new ExtensibleConsumerRecord<>("topic", 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(),
            stringSerializer.serialize("topic", message0));

    // Let consumer record 1 be a large message.
    LargeMessageSegment segment =
        TestUtils.createLargeMessageSegment(UUID.randomUUID(), 0, 2, 20, 10);
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord1 =
        new ExtensibleConsumerRecord<>("topic", 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), segment.segmentArray());
    consumerRecord1.header(HeaderKeySpace.LARGE_MESSAGE_SEGMENT_HEADER, segment.segmentHeader());

    // Construct the consumer records.
    List<ExtensibleConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    recordList.add(consumerRecord0);
    recordList.add(consumerRecord1);

    Collection<ExtensibleConsumerRecord<byte[], byte[]>> filteredRecords = consumerRecordsProcessor.process(recordList);
    ExtensibleConsumerRecord<byte[], byte[]> filteredXRecord = filteredRecords.iterator().next();
    assertEquals(filteredRecords.size(), 1, "Only one record should be there after filtering.");
    assertEquals(consumerRecord0.topic(), filteredXRecord.topic(), "Topic should match");
    assertEquals(consumerRecord0.partition(), filteredXRecord.partition(), "partition should match");
    assertEquals(consumerRecord0.key(), filteredXRecord.key(), "key should match");
    assertEquals(consumerRecord0.offset(), filteredXRecord.offset(), "Offset should match");
    assertEquals(consumerRecord0.value(), "message0".getBytes(), "\"message0\" should be the value");
  }

  @Test
  public void testCorrectness() {
    ConsumerRecordsProcessor consumerRecordsProcessor = createConsumerRecordsProcessor();
    Collection<ExtensibleConsumerRecord<byte[], byte[]>> processedRecords = consumerRecordsProcessor.process(getConsumerRecords());
    assertEquals(processedRecords.size(), 4, "There should be 4 records");
    Iterator<ExtensibleConsumerRecord<byte[], byte[]>> iter = processedRecords.iterator();
    assertEquals(iter.next().offset(), 0, "Message offset should b 0");
    assertEquals(iter.next().offset(), 2, "Message offset should b 2");
    assertEquals(iter.next().offset(), 4, "Message offset should b 4");
    assertEquals(iter.next().offset(), 5, "Message offset should b 5");
  }

  @Test
  public void testSafeOffsetWithoutLargeMessage() throws IOException {
    Serializer<String> stringSerializer = new StringSerializer();
    ConsumerRecordsProcessor consumerRecordsProcessor = createConsumerRecordsProcessor();

    // Let consumer record 0 and 1 be a normal record.
    // Let consumer record 0 be a normal record.
    byte[] message0Bytes = stringSerializer.serialize("topic", "message0");
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord0 =
        new ExtensibleConsumerRecord<>("topic", 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), message0Bytes);

    // Let consumer record 1 be a normal message.
    byte[] message1Bytes = stringSerializer.serialize("topic", "message1");
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord1 =
        new ExtensibleConsumerRecord<>("topic", 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), message1Bytes);

    // Construct the consumer records.
    TopicPartition tp = new TopicPartition("topic", 0);
    List<ExtensibleConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    recordList.add(consumerRecord0);
    recordList.add(consumerRecord1);

    consumerRecordsProcessor.process(recordList);
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
    ConsumerRecordsProcessor consumerRecordsProcessor = createConsumerRecordsProcessor();
    Collection<ExtensibleConsumerRecord<byte[], byte[]>> processedRecords =
      consumerRecordsProcessor.process(getConsumerRecords());
    assertEquals(processedRecords.size(), 4);

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
    // Create two large messages.
    MessageSplitter splitter = new MessageSplitterImpl(500, new UUIDFactoryImpl());

    ConsumerRecordsProcessor consumerRecordsProcessor = createConsumerRecordsProcessor();
    consumerRecordsProcessor.process(getConsumerRecords());
    // The offset tracker now has 2, 4, 5 in it.
    TopicPartition tp = new TopicPartition("topic", 0);

    byte[] largeMessage1Bytes = stringSerializer.serialize("topic", TestUtils.getRandomString(600));
    ExtensibleProducerRecord<byte[], byte[]> largeMessage =
        new ExtensibleProducerRecord<>("topic", tp.partition(), null, "key".getBytes(), largeMessage1Bytes);
    Iterator<ExtensibleProducerRecord<byte[], byte[]>> splitLargeMessage = splitter.split(largeMessage).iterator();

    // Test evict
    List<ExtensibleConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    // Let consumer record 6 be a large message segment.
    ExtensibleProducerRecord<byte[], byte[]> producerRecord6S0 = splitLargeMessage.next();
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord6 =
      TestUtils.producerRecordToConsumerRecord(producerRecord6S0, 6, 0L, TimestampType.CREATE_TIME, 0, 0);

    // Let consumer record 7 be a normal record.
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord7 =
        new ExtensibleConsumerRecord<>("topic", 0, 7, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(),
            stringSerializer.serialize("topic", "message7"));
    // Let consumer record 8 completes consumer record 6
    ExtensibleProducerRecord<byte[], byte[]> producerRecord6S1 = splitLargeMessage.next();
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord8 =
      TestUtils.producerRecordToConsumerRecord(producerRecord6S1, 8, 0L, TimestampType.CREATE_TIME, 0, 0);

    recordList.add(consumerRecord6);
    recordList.add(consumerRecord7);
    recordList.add(consumerRecord8);

    consumerRecordsProcessor.process(recordList);

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
    ConsumerRecordsProcessor consumerRecordsProcessor = createConsumerRecordsProcessor();
    consumerRecordsProcessor.process(getConsumerRecords());

    TopicPartition tp = new TopicPartition("topic", 0);
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 4L), 3, "Starting offset of large message 2 should be 3");
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 5L), 1, "Starting offset of large message 1 should be 1");
    //starting offset of message 0 is not known because it is not a large message
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 0L), 0, "Starting offset of normal message 0 should be 6");
  }

  @Test
  public void testStartingOffsetWithoutMessages() throws IOException {
    ConsumerRecordsProcessor consumerRecordsProcessor = createConsumerRecordsProcessor();

    TopicPartition tp = new TopicPartition("topic", 0);
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 100L), 100, "Should return 100 because there are no " +
        "large messages in the partition.");
  }

  @Test(expectedExceptions = OffsetNotTrackedException.class)
  public void testStartingOffsetWithNormalMessages() throws IOException {
    Serializer<String> stringSerializer = new StringSerializer();
    ConsumerRecordsProcessor consumerRecordsProcessor = createConsumerRecordsProcessor();

    // Let consumer record 0 be a normal record.
    byte[] message0Bytes = stringSerializer.serialize("topic", "message0");
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord0 =
        new ExtensibleConsumerRecord<>("topic", 0, 100L, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "key".getBytes(), message0Bytes);

    // Construct the consumer records.
    List<ExtensibleConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    recordList.add(consumerRecord0);

    consumerRecordsProcessor.process(recordList);

    TopicPartition tp = new TopicPartition("topic", 0);
    assertEquals(consumerRecordsProcessor.startingOffset(tp, 100L), 100, "Should return 100 because there are no " +
        "large messages in the partition.");

    // Should throw exception when an offset cannot be found by the offset tracker.
    consumerRecordsProcessor.startingOffset(tp, 0L);
  }

  @Test
  public void testLastDelivered() {
    ConsumerRecordsProcessor consumerRecordsProcessor = createConsumerRecordsProcessor();
    consumerRecordsProcessor.process(getConsumerRecords());

    assertEquals(consumerRecordsProcessor.delivered(new TopicPartition("topic", 0)).longValue(), 5L,
                 "The last deivered message should be 5");

    assertNull(consumerRecordsProcessor.delivered(new TopicPartition("topic", 1)));
  }

  /**
   * Generates the sequence of records:
   * <pre>
   *   0 m0   -- not a large message
   *   1 m1s0
   *   2 m2   -- not a a large message
   *   3 m3s0
   *   4 m3s1 -- completes m3
   *   5 m1s1 -- completes m1
   * </pre>
   */
  private List<ExtensibleConsumerRecord<byte[], byte[]>> getConsumerRecords() {
    Serializer<String> stringSerializer = new StringSerializer();
    // Create two large messages.

    MessageSplitter splitter = new MessageSplitterImpl(500, new UUIDFactoryImpl());
    int partition  = 0;

    byte[] largeMessage1Bytes = stringSerializer.serialize("topic", TestUtils.getRandomString(600));
    ExtensibleProducerRecord<byte[], byte[]> largeRecord1 =
        new ExtensibleProducerRecord<>("topic", partition, null, "key".getBytes(), largeMessage1Bytes);
    Iterator<ExtensibleProducerRecord<byte[], byte[]>> splitLargeMessage1 = splitter.split(largeRecord1).iterator();

    byte[] largeMessage2Bytes = stringSerializer.serialize("topic", TestUtils.getRandomString(600));
    ExtensibleProducerRecord<byte[], byte[]> largeRecord2 =
        new ExtensibleProducerRecord<>("topic", partition, null, "key".getBytes(), largeMessage2Bytes);
    Iterator<ExtensibleProducerRecord<byte[], byte[]>> splitLargeMessage2 = splitter.split(largeRecord2).iterator();

    // Let consumer record 0 be a normal record.
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord0 =
        new ExtensibleConsumerRecord<>("topic", 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, 0,
          "key".getBytes(), stringSerializer.serialize("topic", "message0"));
    // Let consumer record 1 be a large message segment
    ExtensibleProducerRecord<byte[], byte[]> producerRecord1S0 = splitLargeMessage1.next();
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord1 =
      TestUtils.producerRecordToConsumerRecord(producerRecord1S0, 1, 0L, TimestampType.CREATE_TIME, 0, 0);

    // Let consumer record 2 be a normal message
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord2 =
        new ExtensibleConsumerRecord<>("topic", 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, 0,
          "key".getBytes(), stringSerializer.serialize("topic", "message1"));
    // Let record 3 be a new large message segment
    ExtensibleProducerRecord<byte[], byte[]> producerRecord2S0 = splitLargeMessage2.next();
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord3 =
      TestUtils.producerRecordToConsumerRecord(producerRecord2S0, 3, 0L, TimestampType.CREATE_TIME, 0, 0);

    // let record 4 completes record 3
    ExtensibleProducerRecord<byte[], byte[]> producerRecord2S1 = splitLargeMessage2.next();
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord4 =
      TestUtils.producerRecordToConsumerRecord(producerRecord2S1, 4, 0L, TimestampType.CREATE_TIME, 0, 0);

    // let record 5 completes record 1
    ExtensibleProducerRecord<byte[], byte[]> producerRecord1S1 = splitLargeMessage1.next();
    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord5 =
      TestUtils.producerRecordToConsumerRecord(producerRecord1S1, 5, 0L, TimestampType.CREATE_TIME, 0, 0);

    // Construct the consumer records.
    List<ExtensibleConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    recordList.add(consumerRecord0);
    recordList.add(consumerRecord1);
    recordList.add(consumerRecord2);
    recordList.add(consumerRecord3);
    recordList.add(consumerRecord4);
    recordList.add(consumerRecord5);
    return recordList;
  }

  private ConsumerRecordsProcessor createConsumerRecordsProcessor() {
    MessageAssembler assembler = new MessageAssemblerImpl(5000, 100, false);
    DeliveredMessageOffsetTracker deliveredMessageOffsetTracker = new DeliveredMessageOffsetTracker(4);
    return new ConsumerRecordsProcessor(assembler, deliveredMessageOffsetTracker);
  }

}
