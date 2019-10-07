/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.auditing.NoOpAuditor;
import com.linkedin.kafka.clients.largemessage.DefaultSegmentDeserializer;
import com.linkedin.kafka.clients.largemessage.DefaultSegmentSerializer;
import com.linkedin.kafka.clients.largemessage.MessageSplitter;
import com.linkedin.kafka.clients.largemessage.MessageSplitterImpl;
import com.linkedin.kafka.clients.largemessage.errors.ConsumerRecordsProcessingException;
import com.linkedin.kafka.clients.producer.LiKafkaProducer;
import com.linkedin.kafka.clients.producer.UUIDFactory;
import com.linkedin.kafka.clients.utils.LiKafkaClientsTestUtils;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import com.linkedin.kafka.clients.utils.tests.KafkaTestUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Integration test for LiKafkaConsumer
 */
public class LiKafkaConsumerIntegrationTest extends AbstractKafkaClientsIntegrationTestHarness {

  private final int MESSAGE_COUNT = 1000;
  private Random _random;
  private final String TOPIC1 = "topic1";
  private final String TOPIC2 = "topic2";
  private final int NUM_PRODUCER = 2;
  private final int THREADS_PER_PRODUCER = 2;
  private final int NUM_PARTITIONS = 4;
  private final int MAX_SEGMENT_SIZE = 200; //must match value in AbstractKafkaClientsIntegrationTestHarness.getProducerProperties()
  private final int SYNTHETIC_PARTITION_0 = 0;
  private final int SYNTHETIC_PARTITION_1 = 1;
  private ConcurrentMap<String, String> _messages;

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();
    props.setProperty(KafkaConfig.NumPartitionsProp(), Integer.toString(NUM_PARTITIONS));
    props.setProperty(KafkaConfig.LogRetentionTimeMillisProp(), "" + TimeUnit.DAYS.toMillis(1));
    return props;
  }

  /**
   * This test will have a topic with some partitions having interleaved large messages as well as some ordinary
   * sized messages. The topic will be used for all the sub-tests.
   */
  @BeforeMethod
  @Override
  public void setUp() {
    super.setUp();
  }

  @AfterMethod
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void advanceOffsetsWhenLargeMessageCanNotBeAssembled() throws Exception {
    String topic = "testOffsetDeliveryIssue";
    createTopic(topic);
    produceSyntheticMessages(topic);

    // Bury under a bunch of non-large messages, some of the internal trimming logic is only triggered when
    // we get large message segments and so getting normal messages won't trigger it.
    MessageSplitter splitter = new MessageSplitterImpl(MAX_SEGMENT_SIZE,
        new DefaultSegmentSerializer(),
        new UUIDFactory.DefaultUUIDFactory<>());

    int buriedMessageCount = 1_000;
    int syntheticMessageCount = 10;
    int totalMessageCount = buriedMessageCount + syntheticMessageCount;
    Producer<byte[], byte[]> extraMessagesProducer = createRawProducer();
    for (int i = 0; i < buriedMessageCount; i++) {
      UUID messageId = LiKafkaClientsUtils.randomUUID();
      String message = LiKafkaClientsTestUtils.getRandomString(MAX_SEGMENT_SIZE / 2);
      List<ProducerRecord<byte[], byte[]>> split =
          splitter.split(topic, SYNTHETIC_PARTITION_0, messageId, message.getBytes());
      assertEquals(split.size(), 1);
      extraMessagesProducer.send(split.get(0)).get();
    }
    extraMessagesProducer.flush();

    // Commit offset so that M0 can not be reassembled
    Properties openSourceConsumerProperties = getConsumerProperties(new Properties());
    openSourceConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testBadAssemblyAdvance");
    openSourceConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    seedBadOffset(topic, openSourceConsumerProperties, 1, -(totalMessageCount - 1));

    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(openSourceConsumerProperties)) {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(tp);
      assertEquals(offsetAndMetadata.offset(), 1);
    }

    // Consume messages until we reach the end.
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, openSourceConsumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.setProperty(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_CONFIG, buriedMessageCount + "");
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      consumer.assign(Collections.singleton(tp));
      long deadline = System.currentTimeMillis() + 4_000;
      while (System.currentTimeMillis() < deadline && consumer.position(tp) != totalMessageCount) {
        consumer.poll(1_000);
      }
      consumer.commitSync();
    }

    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(openSourceConsumerProperties)) {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(tp);
      assertEquals(offsetAndMetadata.offset(), totalMessageCount);
    }
  }

  @Test
  public void advanceOffsetsWhenMessagesNotDelivered() throws Exception {
    String topic = "testUle";
    createTopic(topic);
    produceSyntheticMessages(topic);

    // Commit offset with very large high watermark
    Properties openSourceConsumerProperties = getConsumerProperties(new Properties());
    openSourceConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testUleGroup");
    openSourceConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    seedBadOffset(topic, openSourceConsumerProperties, 0, 4_000);

    // Consume messages before committed highwatermark
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testUleGroup");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      consumer.assign(Collections.singleton(tp));
      ConsumerRecords<String, String> consumerRecords = consumer.poll(4_000);
      Assert.assertEquals(consumerRecords.count(), 0);
      consumer.commitSync();
    }

    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(openSourceConsumerProperties)) {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(tp);
      assertEquals(offsetAndMetadata.offset(), 10);
    }
  }

  private void seedBadOffset(String topic, Properties openSourceConsumerProperties, long offset, long diffFromEnd) {
    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(openSourceConsumerProperties)) {
      List<PartitionInfo> partitionsForTopic = kafkaConsumer.partitionsFor(topic);
      List<TopicPartition> tps = new ArrayList<>();
      for (int i = 0; i < partitionsForTopic.size(); i++) {
        tps.add(new TopicPartition(topic, i));
      }
      Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(tps);
      Map<TopicPartition, OffsetAndMetadata> badOffsetMap = new HashMap<>();
      for (Map.Entry<TopicPartition, Long> endOffsetEntry : endOffsets.entrySet()) {
        if (endOffsetEntry.getValue() == null || endOffsetEntry.getValue() == -1) {
          continue;
        }
        long badOffset = endOffsetEntry.getValue() + diffFromEnd;
        OffsetAndMetadata om = new OffsetAndMetadata(offset, badOffset + ",");
        badOffsetMap.put(endOffsetEntry.getKey(), om);
      }
      kafkaConsumer.commitSync(badOffsetMap);
    }
  }
  @Test
  public void testSeek() throws Exception {
    String topic = "testSeek";
    createTopic(topic);
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testSeek");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      consumer.assign(Collections.singleton(tp));

      // Now seek to message offset 0
      consumer.seek(tp, 0);
      verifyMessagesAfterSeek(consumer, Arrays.asList(2L, 4L, 5L, 6L, 7L, 9L));

      // Now seek to message offset 1 which is not a message offset and is earlier than the first delivered message.
      // in this case, the consumer will seek to the safe offset of the partition.
      consumer.seek(tp, 1);
      assertEquals(consumer.position(tp), 0L, "The position should be 0 after the seek.");
      verifyMessagesAfterSeek(consumer, Arrays.asList(2L, 4L, 5L, 6L, 7L, 9L));

      // Now seek to message offset 2
      consumer.seek(tp, 2);
      verifyMessagesAfterSeek(consumer, Arrays.asList(2L, 4L, 5L, 6L, 7L, 9L));

      // Now seek to message offset 3, it is a non message offset. The consumer will actually seek to safe offset
      // of 2. m2 should be ignored and m1 should be delivered.
      consumer.seek(tp, 3);
      verifyMessagesAfterSeek(consumer, Arrays.asList(4L, 5L, 6L, 7L, 9L));

      // Now seek to message offset 4, m1 should be delivered but m2 should not be delivered.
      consumer.seek(tp, 4);
      verifyMessagesAfterSeek(consumer, Arrays.asList(4L, 5L, 6L, 7L, 9L));

      // Now seek to message offset 5, m0 should be delivered but m1 and m2 should not be delivered.
      consumer.seek(tp, 5);
      verifyMessagesAfterSeek(consumer, Arrays.asList(5L, 6L, 7L, 9L));

      // Now seek to message offset 6, m3 should be delivered but m2 should not be delivered.
      consumer.seek(tp, 6);
      assertEquals(consumer.position(tp), 3L);
      verifyMessagesAfterSeek(consumer, Arrays.asList(6L, 7L, 9L));

      // Now seek to message offset 7, m4 should be delivered but m2 should not be delivered.
      consumer.seek(tp, 7);
      verifyMessagesAfterSeek(consumer, Arrays.asList(7L, 9L));

      // Now seek to message offset 8, m5 should be delivered.
      consumer.seek(tp, 8);
      verifyMessagesAfterSeek(consumer, Arrays.asList(9L));

      // Now seek to message offset 9, m5 should be delivered.
      consumer.seek(tp, 9);
      verifyMessagesAfterSeek(consumer, Arrays.asList(9L));
    }
  }

  private void verifyMessagesAfterSeek(LiKafkaConsumer<String, String> consumer,
      List<Long> expectedOffsets) {
    ConsumerRecords<String, String> records;
    for (long expectedOffset : expectedOffsets) {
      records = null;
      while (records == null || records.isEmpty()) {
        records = consumer.poll(10);
      }
      assertEquals(records.count(), 1, "Should return one message");
      assertEquals(records.iterator().next().offset(), expectedOffset, "Message offset should be " + expectedOffset);
    }
  }

  @Test
  public void testCommit() throws Exception {
    String topic = "testCommit";
    createTopic(topic);
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testCommit");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    LiKafkaConsumer<String, String> consumer = createConsumer(props);
    try {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      TopicPartition tp1 = new TopicPartition(topic, SYNTHETIC_PARTITION_1);
      consumer.assign(Arrays.asList(tp, tp1));

      while (consumer.poll(10).isEmpty()) {
        //M2
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(3, ""), "The committed user offset should be 3");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 0, "The committed actual offset should be 0");
      assertEquals(consumer.position(tp1), 2L, "The position of partition 1 should be 2.");
      assertEquals(consumer.committedSafeOffset(tp1).longValue(), 0L, "The committed safe offset for partition 1 should be 0.");
      assertEquals(consumer.committed(tp1).offset(), 2L, "The committed offset for partition 1 should be 2.");

      while (consumer.poll(10).isEmpty()) {
        // M1
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(5, ""), "The committed user offset should be 5");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 0, "The committed actual offset should be 0");
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(4)));
      assertEquals(consumer.committed(tp).offset(), 4, "The committed user offset should 4");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 0, "The committed actual offset should be 0");

      while (consumer.poll(10).isEmpty()) {
        // M0
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(6, ""), "The committed user offset should be 6");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 3, "The committed actual offset should be 3");

      while (consumer.poll(10).isEmpty()) {
        // M3
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(7, ""), "The committed user offset should be 7");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 7, "The committed actual offset should be 7");

      while (consumer.poll(10).isEmpty()) {
        // M4
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(8, ""), "The committed user offset should be 8");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 8, "The committed actual offset should be 8");

      // test commit offset 0
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(0)));
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(0, ""), "The committed user offset should be 0");

      consumer.close();
      consumer = createConsumer(props);
      consumer.assign(Collections.singleton(tp));
      consumer.seekToCommitted(Collections.singleton(tp));
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(8, "new commit")));
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(8, "new commit"));
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 8);
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testCommitWithOffsetMap() throws Exception {
    String topic = "testCommitWithOffsetMap";
    createTopic(topic);
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testCommitWithOffsetMap");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    LiKafkaConsumer<String, String> consumer = createConsumer(props);

    try {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      consumer.assign(Collections.singleton(tp));

      // First test tracked range from 0 to 9
      consumer.seekToBeginning(Collections.singleton(tp));
      while (consumer.position(tp) != 10) {
        consumer.poll(10L);
      }
      // test commit first tracked offset.
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(0L)));
      assertEquals(0L, consumer.committed(tp).offset());
      assertEquals(0L, consumer.committedSafeOffset(tp).longValue());
      // test last consumed offset + 1
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(10L)));
      assertEquals(10L, consumer.committed(tp).offset());
      assertEquals(10L, consumer.committedSafeOffset(tp).longValue());
      // test a delivered offset (offset of M0 is 5L)
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(6L)));
      assertEquals(6L, consumer.committed(tp).offset());
      assertEquals(3L, consumer.committedSafeOffset(tp).longValue());
      // test a non-message offset,
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(3L)));
      assertEquals(3L, consumer.committed(tp).offset());
      assertEquals(0L, consumer.committedSafeOffset(tp).longValue());

      // Now test tracked range from 2 to 9
      consumer.seek(tp, 100L); // clear up internal state.
      consumer.seek(tp, 2L);
      while (consumer.position(tp) != 10) {
        consumer.poll(10L);
      }
      // test commit an offset smaller than the first tracked offset.
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(0L)));
      assertEquals(0L, consumer.committed(tp).offset());
      assertEquals(0L, consumer.committedSafeOffset(tp).longValue());
      // test commit first tracked offset.
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(2L)));
      assertEquals(2L, consumer.committed(tp).offset());
      assertEquals(2L, consumer.committedSafeOffset(tp).longValue());
      // test last consumed offset + 1
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(10L)));
      assertEquals(10L, consumer.committed(tp).offset());
      assertEquals(4L, consumer.committedSafeOffset(tp).longValue());
      // test a delivered offset (offset of M3 is 6L)
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(7L)));
      assertEquals(7L, consumer.committed(tp).offset());
      assertEquals(4L, consumer.committedSafeOffset(tp).longValue());
      // test a non-message offset,
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(3L)));
      assertEquals(3L, consumer.committed(tp).offset());
      assertEquals(3L, consumer.committedSafeOffset(tp).longValue());

    } finally {
      consumer.close();
    }
  }

  @Test(expectedExceptions = TimeoutException.class)
  public void testCommitWithTimeout() throws Exception {
    String topic = "testCommitWithTimeout";
    createTopic(topic);
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testCommitWithTimeout");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    // No auto commmit
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    LiKafkaConsumer<String, String> consumer = createConsumer(props);
    try {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      consumer.assign(Arrays.asList(tp));

      while (consumer.poll(10).isEmpty()) {
      }
      // Shutdown the broker so that offset commit would hang and eventually time out.
      tearDown();
      consumer.commitSync(Duration.ofSeconds(3));
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testSeekToBeginningAndEnd() throws Exception {
    String topic = "testSeekToBeginningAndEnd";
    createTopic(topic);
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testSeekToBeginningAndEnd");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

    LiKafkaConsumer<String, String> tempConsumer = createConsumer(props);
    TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
    // We commit some message to the broker before seek to end. This is to test if the consumer will be affected
    // by committed offsets after seek to beginning or end.
    tempConsumer.assign(Collections.singleton(tp));
    tempConsumer.seek(tp, Long.MAX_VALUE);
    tempConsumer.commitSync();
    tempConsumer.close();
    // Produce a message and ensure it can be consumed.
    Properties producerProps = new Properties();
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props);
        LiKafkaProducer<String, String> producer = createProducer(producerProps)) {
      consumer.assign(Collections.singleton(tp));
      // Seek to beginning, the follow up poll() should ignore the committed high watermark.
      consumer.seekToBeginning(Collections.singleton(tp));
      ConsumerRecords<String, String> records = consumer.poll(1000L);
      assertEquals(records.iterator().next().offset(), 2L, "Should see message offset 2.");

      consumer.seekToEnd(Collections.singleton(tp));
      consumer.poll(10L);
      assertEquals(10L, consumer.position(tp));

      producer.send(new ProducerRecord<>(topic, SYNTHETIC_PARTITION_0, null, "test"));
      producer.close();
      records = consumer.poll(1000L);
      assertEquals(records.iterator().next().offset(), 10L, "Should see message offset 10.");
    }
  }

  /**
   * This test tests the seekToCommitted() behavior with the following synthetic data:
   * 0: M0_SEG0
   * 1: M1_SEG0
   * 2: M2_SEG0(END)
   * 3: M3_SEG0
   * 4: M1_SEG1(END)
   * 5: M0_SEG1(END)
   * 6: M3_SEG1(END)
   * 7: M4_SEG0(END)
   * 8: M5_SEG0
   * 9: M5_SEG1(END)
   */
  @Test
  public void testSeekToCommitted() throws Exception {
    String topic = "testSeekToCommitted";
    createTopic(topic);
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testSeekToCommitted");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    LiKafkaConsumer<String, String> consumer = createConsumer(props);

    try {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      consumer.assign(Collections.singleton(tp));
      ConsumerRecords<String, String> records = waitForData(consumer, 30000); //M2
      assertEquals(records.count(), 1, "Should have consumed 1 message");
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(3, ""), "The committed user offset should be 3");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 0, "The committed actual offset should be 0");

      consumer.close();
      consumer = createConsumer(props);
      consumer.assign(Collections.singleton(tp));
      // This should work the same as seekToCommitted.
      consumer.seek(tp, consumer.committed(tp).offset());
      assertEquals(consumer.position(tp), 0, "The committed safe offset should be 0");

      records = waitForData(consumer, 30000); // M1

      assertEquals(records.count(), 1, "There should be only one record.");
      assertEquals(records.iterator().next().offset(), 4, "The message offset should be 4");

    } finally {
      consumer.close();
    }
  }

  private ConsumerRecords<String, String> waitForData(LiKafkaConsumer<String, String> consumer, long timeout) {
    long now = System.currentTimeMillis();
    long deadline = now + timeout;
    while (System.currentTimeMillis() < deadline) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      if (records != null && records.count() > 0) {
        return records;
      }
    }
    Assert.fail("failed to read any records within time limit");
    return null;
  }

  @Test
  public void testOffsetCommitCallback() throws Exception {
    String topic = "testOffsetCommitCallback";
    createTopic(topic);
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testOffsetCommitCallback");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      consumer.assign(Collections.singleton(tp));
      waitForData(consumer, 30000); // M2
      OffsetAndMetadata committed = commitAndRetrieveOffsets(consumer, tp, null);
      assertEquals(committed, new OffsetAndMetadata(3, ""), "The committed user offset should be 3, instead was " + committed);
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 0, "The committed actual offset should be 0");
      Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
      offsetMap.put(tp, new OffsetAndMetadata(0));
      committed = commitAndRetrieveOffsets(consumer, tp, offsetMap);
      assertEquals(committed, new OffsetAndMetadata(0, ""), "The committed user offset should be 0, instead was " + committed);
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 0, "The committed actual offset should be 0");
    }
  }

  private OffsetAndMetadata commitAndRetrieveOffsets(
      LiKafkaConsumer<String, String> consumer,
      TopicPartition tp, Map<TopicPartition,
      OffsetAndMetadata> offsetMap) throws Exception {
    final AtomicBoolean callbackFired = new AtomicBoolean(false);
    final AtomicReference<Exception> offsetCommitIssue = new AtomicReference<>(null);
    OffsetAndMetadata committed = null;
    long now = System.currentTimeMillis();
    long deadline = now + TimeUnit.MINUTES.toMillis(1);
    while (System.currentTimeMillis() < deadline) {
      //call commitAsync, wait for a NON-NULL return value (see https://issues.apache.org/jira/browse/KAFKA-6183)
      OffsetCommitCallback commitCallback = new OffsetCommitCallback() {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap, Exception e) {
          if (e != null) {
            offsetCommitIssue.set(e);
          }
          callbackFired.set(true);
        }
      };
      if (offsetMap != null) {
        consumer.commitAsync(offsetMap, commitCallback);
      } else {
        consumer.commitAsync(commitCallback);
      }
      while (!callbackFired.get()) {
        consumer.poll(20);
      }
      Assert.assertNull(offsetCommitIssue.get(), "offset commit failed");
      committed = consumer.committed(tp);
      if (committed != null) {
        break;
      }
      Thread.sleep(100);
    }
    assertNotNull(committed, "unable to retrieve committed offsets within timeout");
    return committed;
  }

  @Test
  public void testCommittedOnOffsetsCommittedByRawConsumer() throws Exception {
    String topic = "testCommittedOnOffsetsCommittedByRawConsumer";
    TopicPartition tp = new TopicPartition(topic, 0);
    createTopic(topic);
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testCommittedOnOffsetsCommittedByRawConsumer");

    try (LiKafkaConsumer<String, String> consumer = createConsumer(props);
        Consumer<byte[], byte[]> rawConsumer = new KafkaConsumer<>(getConsumerProperties(props))) {
      consumer.assign(Collections.singleton(tp));
      rawConsumer.assign(Collections.singleton(tp));
      // Test commit an offset without metadata
      rawConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(0L)));
      OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
      assertEquals(offsetAndMetadata.offset(), 0L);
      assertEquals(offsetAndMetadata.metadata(), "");
    }

    try (LiKafkaConsumer<String, String> consumer = createConsumer(props);
        Consumer<byte[], byte[]> rawConsumer = new KafkaConsumer<>(getConsumerProperties(props))) {
      consumer.assign(Collections.singleton(tp));
      rawConsumer.assign(Collections.singleton(tp));
      // Test commit an offset with metadata containing no comma
      rawConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(1L, "test")));
      OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
      assertEquals(offsetAndMetadata.offset(), 1L);
      assertEquals(offsetAndMetadata.metadata(), "test");
    }

    try (LiKafkaConsumer<String, String> consumer = createConsumer(props);
        Consumer<byte[], byte[]> rawConsumer = new KafkaConsumer<>(getConsumerProperties(props))) {
      consumer.assign(Collections.singleton(tp));
      rawConsumer.assign(Collections.singleton(tp));
      // Test commit an offset with metadata containing a comma
      rawConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(2L, "test,test")));
      OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
      assertEquals(offsetAndMetadata.offset(), 2L);
      assertEquals(offsetAndMetadata.metadata(), "test,test");
    }
  }

  @Test
  public void testSeekAfterAssignmentChange() throws Exception {
    createTopic(TOPIC1);
    createTopic(TOPIC2);
    produceRecordsWithKafkaProducer();
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty("group.id", "testSeekAfterAssignmentChange");
    // Make sure we start to consume from the beginning
    props.setProperty("auto.offset.reset", "earliest");
    // Consumer at most 100 messages per poll
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    // Set max fetch size to a small value
    props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "2000");
    LiKafkaConsumer<String, String> consumer = createConsumer(props);
    TopicPartition tp0 = new TopicPartition(TOPIC1, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC1, 1);

    consumer.assign(Arrays.asList(tp0, tp1));
    // Consume some messages
    ConsumerRecords<String, String> records = null;
    while (records == null || records.isEmpty()) {
      records = consumer.poll(5000);
    }

    consumer.assign(Collections.singleton(tp1));
    records = null;
    while (records == null || records.isEmpty()) {
      records = consumer.poll(5000);
    }
    // we should be able to seek on tp 0 after assignment change.
    consumer.assign(Arrays.asList(tp0, tp1));
    assertEquals(consumer.safeOffset(tp0), null, "The safe offset for " + tp0 + " should be null now.");
    // We should be able to seek to 0 freely.
    consumer.seek(tp0, 0);
  }

  @Test
  public void testUnsubscribe() throws Exception {
    createTopic(TOPIC1);
    createTopic(TOPIC2);
    produceRecordsWithKafkaProducer();
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty("group.id", "testUnsubscribe");
    // Make sure we start to consume from the beginning.
    props.setProperty("auto.offset.reset", "earliest");
    LiKafkaConsumer<String, String> consumer = createConsumer(props);
    TopicPartition tp = new TopicPartition(TOPIC1, 0);
    consumer.subscribe(Collections.singleton(TOPIC1));

    try {
      ConsumerRecords<String, String> records = null;
      while (records == null || !records.isEmpty()) {
        records = consumer.poll(5000);
      }

      // Seek forward should work.
      consumer.seek(tp, 100000L);
      // After unsubscribe an IllegalStateException should be seen.
      consumer.unsubscribe();
      try {
        consumer.seek(tp, 100000L);
        fail();
      } catch (IllegalStateException lse) {
        // let it go
      }
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testPosition() throws Exception {
    String topic = "testSeek";
    createTopic(topic);
    TopicPartition tp = new TopicPartition(topic, 0);
    TopicPartition tp1 = new TopicPartition(topic, 1);
    produceSyntheticMessages(topic);

    // Reset to earliest
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPosition1");
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      consumer.assign(Arrays.asList(tp, tp1));
      assertEquals(0, consumer.position(tp));
    }

    // Reset to latest
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPosition2");
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      consumer.assign(Arrays.asList(tp, tp1));
      assertEquals(consumer.position(tp), 10);
    }

    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPosition3");
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      consumer.assign(Arrays.asList(tp, tp1));
      consumer.position(tp);
      fail("Should have thrown NoOffsetForPartitionException");
    } catch (NoOffsetForPartitionException nofpe) {
      // let it go.
    }
  }

  /**
   * This is an aggressive rebalance test.
   * There are two consumers in the same group consuming from two topics.
   * The two topics contains mostly large messages.
   * The two consumers will change their subscription frequently to trigger rebalances.
   * The test makes sure that all the messages are consumed exactly once.
   */
  @Test
  public void testRebalance() throws Throwable {
    createTopic(TOPIC1);
    createTopic(TOPIC2);
    produceRecordsWithKafkaProducer(true);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testRebalance");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Consumer at most 100 messages per poll
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    // Set max fetch size to a small value
    props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "2000");
    // Set heartbeat interval to a small value
    props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10");
    // Enable auto offset commit so the offsets will be committed during rebalance.
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.setProperty(LiKafkaConsumerConfig.MAX_TRACKED_MESSAGES_PER_PARTITION_CONFIG, "8000");

    props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
    LiKafkaConsumer<String, String> consumer0 = createConsumer(props);

    props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer1");
    LiKafkaConsumer<String, String> consumer1 = createConsumer(props);

    int numSent = _messages.size();
    final Map<String, String> messageUnseen = new ConcurrentSkipListMap<>(_messages);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    Thread thread0 = new RebalanceTestConsumerThread(consumer0, messageUnseen, 0, error);
    Thread thread1 = new RebalanceTestConsumerThread(consumer1, messageUnseen, 1, error);

    long threadsStart = System.currentTimeMillis();
    thread0.start();
    thread1.start();

    try {
      thread0.join();
      thread1.join();
    } catch (InterruptedException e) {
      // Do nothing
    } finally {
      consumer0.close();
      consumer1.close();
    }

    if (error.get() != null) {
      throw error.get();
    }
    long threadsRuntime = System.currentTimeMillis() - threadsStart;
    int numUnseen = messageUnseen.size();
    String err = "Messages send: " + numSent + " Messages unseen after " + threadsRuntime + " ms: " + numUnseen + " ("
        + messageUnseen.keySet() + ")";
    assertEquals(numUnseen, 0, err);
  }

  /**
   * This test mimics the following sequence:
   * 1. User started a consumer to consume
   * 2. Consumer commits offset according to the message it receives.
   * 3. A consumer die/close at some point
   * 4. Another consumer in the same group starts and try to resume from committed offsets.
   * The partitions that is consumed should have many interleaved messages. After stopping and resuming consumption,
   * the consumers should not miss any message.
   * <p>
   * This test indirectly tested the following methods:
   * <li>{@link LiKafkaConsumer#subscribe(java.util.Collection)}
   * <li>{@link LiKafkaConsumer#poll(long)}
   * <li>{@link LiKafkaConsumer#commitSync()}
   * <li>{@link LiKafkaConsumer#close()}
   *
   * @throws InterruptedException
   */
  @Test
  public void testCommitAndResume() throws Exception {
    createTopic(TOPIC1);
    createTopic(TOPIC2);
    produceRecordsWithKafkaProducer();
    Properties props = new Properties();
    // Make sure we start to consume from the beginning.
    props.setProperty("auto.offset.reset", "earliest");
    // All the consumers should have the same group id.
    props.setProperty("group.id", "testCommitAndResume");
    // Reduce the fetch size for each partition to make sure we will poll() multiple times.
    props.setProperty("max.partition.fetch.bytes", "6400");

    props.setProperty("enable.auto.commit", "false");
    LiKafkaConsumer<String, String> consumer = createConsumer(props);

    try {
      // Subscribe to the partitions.
      consumer.subscribe(Arrays.asList(TOPIC1, TOPIC2));

      // Create a new map to record unseen messages which initially contains all the produced _messages.
      Map<String, String> messagesUnseen = new ConcurrentSkipListMap<>(new MessageIdComparator());
      messagesUnseen.putAll(_messages);

      long startTime = System.currentTimeMillis();
      int numMessagesConsumed = 0;
      int lastCommitAndResume = 0;
      int numCommitAndResume = 0;
      Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
      ConsumerRecords<String, String> records;
      long deadline = startTime + TimeUnit.MINUTES.toMillis(5);
      while (!messagesUnseen.isEmpty() && System.currentTimeMillis() < deadline) {
        records = consumer.poll(500);

        for (ConsumerRecord<String, String> record : records) {
          String messageId = messageId(record.topic(), record.partition(), record.offset());
          // We should not see any duplicate message.
          String origMessage = messagesUnseen.remove(messageId);
          assertEquals(record.value(), origMessage, "Message with id \"" + messageId + "\" should be the same.");
          offsetMap.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
          numMessagesConsumed++;
          // We try to stop and recreate a consumer every MESSAGE_COUNT messages and at most stop and resume 4 times.
          if (lastCommitAndResume + (NUM_PRODUCER * THREADS_PER_PRODUCER * MESSAGE_COUNT) / 4 < numMessagesConsumed &&
              numCommitAndResume < 4 && offsetMap.size() == 2 * NUM_PARTITIONS) {
            consumer.commitSync(offsetMap);
            consumer.close();
            offsetMap.clear();
            consumer = createConsumer(props);
            consumer.subscribe(Arrays.asList(TOPIC1, TOPIC2));
            lastCommitAndResume = numMessagesConsumed;
            numCommitAndResume++;
            break;
          }
        }
      }
      assertEquals(messagesUnseen.size(), 0, "Stop and resumed " + numCommitAndResume + "times, consumed " +
          numMessagesConsumed + " messages. ");
    } finally {
      consumer.close();
    }
  }

  /**
   * Test search offset by timestamp
   */
  @Test
  public void testSearchOffsetByTimestamp() throws Exception {
    Properties props = new Properties();
    // Make sure we start to consume from the beginning.
    props.setProperty("auto.offset.reset", "earliest");
    // All the consumers should have the same group id.
    props.setProperty("group.id", "testSearchOffsetByTimestamp");
    props.setProperty("enable.auto.commit", "false");
    createTopic(TOPIC1);
    createTopic(TOPIC2);
    produceRecordsWithKafkaProducer();

    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
      // Consume the messages with largest 10 timestamps.
      for (int i = 0; i < NUM_PARTITIONS; i++) {
        timestampsToSearch.put(new TopicPartition(TOPIC1, i), (long) MESSAGE_COUNT - 10);
      }
      consumer.assign(timestampsToSearch.keySet());
      Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampsToSearch);
      for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsets.entrySet()) {
        assertNotNull(entry.getValue(), "Failed to find offset for topic partition " + entry.getKey() +
            " for timestamp " + timestampsToSearch.get(entry.getKey()));
        consumer.seek(entry.getKey(), entry.getValue().offset());
      }

      int i = 0;
      Map<Long, Integer> messageCount = new HashMap<>();
      long start = System.currentTimeMillis();
      while (i < NUM_PRODUCER * THREADS_PER_PRODUCER * 10 && System.currentTimeMillis() < start + 20000) {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
        for (ConsumerRecord record : consumerRecords) {
          if (record.timestamp() >= MESSAGE_COUNT - 10) {
            int count = messageCount.getOrDefault(record.timestamp(), 0);
            messageCount.put(record.timestamp(), count + 1);
            i++;
          }
        }
      }
      assertEquals(i, NUM_PRODUCER * THREADS_PER_PRODUCER * 10);
      assertEquals(messageCount.size(), 10, "Should have consumed messages of all 10 timestamps");
      int expectedCount = NUM_PRODUCER * THREADS_PER_PRODUCER;
      for (Integer count : messageCount.values()) {
        assertEquals(count.intValue(), expectedCount, "Each partition should have " + expectedCount + " messages");
      }
    }
  }

  private LiKafkaConsumer<byte[], byte[]> createConsumerForExceptionProcessingTest() {
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testCommit");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    return new LiKafkaConsumerImpl<>(getConsumerProperties(props),
        new ByteArrayDeserializer(),
        new Deserializer<byte[]>() {
          @Override
          public void configure(Map<String, ?> configs, boolean isKey) {

          }

          @Override
          public byte[] deserialize(String topic, byte[] data) {
            // Throw exception when deserializing
            if (new String(data).startsWith(KafkaTestUtils.EXCEPTION_MESSAGE)) {
              throw new SerializationException("intentional exception by marker payload");
            }
            return data;
          }

          @Override
          public void close() {

          }
        }, new DefaultSegmentDeserializer(), new NoOpAuditor<>());
  }

  private void testExceptionProcessingByFunction(String topic, LiKafkaConsumer<byte[], byte[]> consumer,
      BiConsumer<LiKafkaConsumer<byte[], byte[]>, TopicPartition> testFunction) throws Exception {
    try {
      consumer.subscribe(Collections.singleton(topic));
      ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
      while (records.isEmpty()) {
        records = consumer.poll(Duration.ofMillis(10));
      }
      assertEquals(records.count(), 4, "Only the first message should be returned");
      assertEquals(records.iterator().next().offset(), 2L, "The offset of the first message should be 2.");
      assertEquals(consumer.position(new TopicPartition(topic, 0)), 7L, "The position should be 7");

      testFunction.accept(consumer, new TopicPartition(topic, 0));
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testExceptionHandlingAndProcessing() {
    List<BiConsumer<LiKafkaConsumer<byte[], byte[]>, TopicPartition>> testFuncList = new ArrayList<>(6);
    testFuncList.add((consumer, tp) -> {
      try {
        consumer.poll(Duration.ofMillis(100));
        fail("Should have thrown exception.");
      } catch (ConsumerRecordsProcessingException crpe) {
        // expected
      }

      assertEquals(consumer.position(tp), 8L, "The position should be 8");
      ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
      while (records.isEmpty()) {
        records = consumer.poll(Duration.ofMillis(10));
      }
      assertEquals(records.count(), 1, "There should be four messages left.");
    });

    testFuncList.add((consumer, tp) -> {
      consumer.pause(Collections.singleton(tp));
      consumer.poll(Duration.ofMillis(1000));
      consumer.resume(Collections.singleton(tp));
      assertEquals(consumer.position(tp), 7L, "The position should be 7");

      ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
      try {
        while (records.isEmpty()) {
          records = consumer.poll(Duration.ofMillis(1000));
        }
      } catch (ConsumerRecordsProcessingException crpe) {
        // expected
      }
      assertEquals(consumer.position(tp), 8L, "The position should be 8");
    });

    testFuncList.add((consumer, tp) -> {
      consumer.seek(tp, 6);
      try {
        consumer.poll(Duration.ofMillis(1000));
      } catch (Exception e) {
        fail("Unexpected exception");
      }
      assertEquals(consumer.position(tp), 7L, "The position should be 7");
    });

    testFuncList.add((consumer, tp) -> {
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(4L)));
      consumer.seekToCommitted(Collections.singleton(tp));
      try {
        consumer.poll(Duration.ofMillis(1000));
      } catch (Exception e) {
        fail("Unexpected exception");
      }
      assertEquals(consumer.position(tp), 7L, "The position should be 7");
    });

    testFuncList.add((consumer, tp) -> {
      consumer.seekToBeginning(Collections.singleton(tp));
      int recordsRead = 0;
      long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
      while (recordsRead < 4 && System.currentTimeMillis() < deadline) {
        try {
          recordsRead += consumer.poll(Duration.ofMillis(10)).count();
        } catch (Exception e) {
          fail("Unexpected exception");
        }
      }
      Assert.assertEquals(recordsRead, 4, "expected to read 4 records. read " + recordsRead);
      assertEquals(consumer.position(tp), 7L, "The position should be 7");
    });

    testFuncList.add((consumer, tp) -> {
      consumer.seekToEnd(Collections.singleton(tp));
      try {
        consumer.poll(Duration.ofMillis(1000));
      } catch (Exception e) {
        fail("Unexpected exception");
      }
      assertEquals(consumer.position(tp), 10L, "The position should be 10");
    });

    testFuncList.forEach(
        testFunc -> {
          String topic = UUID.randomUUID().toString();
          try {
            createTopic(topic);
          } catch (Exception e) {
            fail("unable to create topic " + topic, e);
          }
          produceSyntheticMessages(topic);
          LiKafkaConsumer<byte[], byte[]> consumer = createConsumerForExceptionProcessingTest();
          try {
            testExceptionProcessingByFunction(topic, consumer, testFunc);
          } catch (Exception e) {
            fail("failed with unexpected exception", e);
          }
        }
    );
  }

  @Test
  public void testExceptionInProcessingLargeMessage() throws Exception {
    String topic = "testExceptionInProcessing";
    createTopic(topic);
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testCommit");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    LiKafkaConsumer<byte[], byte[]> consumer =
        new LiKafkaConsumerImpl<>(getConsumerProperties(props),
            new ByteArrayDeserializer(),
            new Deserializer<byte[]>() {
              int numMessages = 0;
              @Override
              public void configure(Map<String, ?> configs, boolean isKey) {

              }

              @Override
              public byte[] deserialize(String topic, byte[] data) {
                // Throw exception when deserializing the second message.
                numMessages++;
                if (numMessages == 2) {
                  throw new SerializationException();
                }
                return data;
              }

              @Override
              public void close() {

              }
            }, new DefaultSegmentDeserializer(), new NoOpAuditor<>());
    try {
      consumer.subscribe(Collections.singleton(topic));
      ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
      while (records.isEmpty()) {
        records = consumer.poll(1000);
      }
      assertEquals(records.count(), 1, "Only the first message should be returned");
      assertEquals(records.iterator().next().offset(), 2L, "The offset of the first message should be 2.");
      assertEquals(consumer.position(new TopicPartition(topic, 0)), 4L, "The position should be 4");

      try {
        consumer.poll(1000);
        fail("Should have thrown exception.");
      } catch (ConsumerRecordsProcessingException crpe) {
        // let it go
      }
      assertEquals(consumer.position(new TopicPartition(topic, 0)), 5L, "The position should be 5");
      records = ConsumerRecords.empty();
      while (records.isEmpty()) {
        records = consumer.poll(1000);
      }
      assertEquals(records.count(), 4, "There should be four messages left.");
      assertEquals(records.iterator().next().offset(), 5L, "The first offset should 5");
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testGiganticLargeMessages() throws Exception {
    MessageSplitter splitter = new MessageSplitterImpl(MAX_SEGMENT_SIZE,
        new DefaultSegmentSerializer(),
        new UUIDFactory.DefaultUUIDFactory<>());

    String topic = "testGiganticLargeMessages";
    createTopic(topic);
    TopicPartition tp = new TopicPartition(topic, 0);
    Collection<TopicPartition> tps = new ArrayList<>(Collections.singletonList(tp));

    //send 2 interleaved gigantic msgs

    Producer<byte[], byte[]> producer = createRawProducer();
    // M0, 20 segments
    UUID messageId0 = LiKafkaClientsUtils.randomUUID();
    String message0 = KafkaTestUtils.getRandomString(20 * MAX_SEGMENT_SIZE);
    List<ProducerRecord<byte[], byte[]>> m0Segs = splitter.split(topic, 0, messageId0, message0.getBytes());
    // M1, 30 segments
    UUID messageId1 = LiKafkaClientsUtils.randomUUID();
    String message1 = KafkaTestUtils.getRandomString(30 * MAX_SEGMENT_SIZE);
    List<ProducerRecord<byte[], byte[]>> m1Segs = splitter.split(topic, 0, messageId1, message1.getBytes());

    List<ProducerRecord<byte[], byte[]>> interleaved = interleave(m0Segs, m1Segs);
    for (ProducerRecord<byte[], byte[]> rec : interleaved) {
      producer.send(rec).get();
    }

    //create a consumer with not enough memory to assemble either

    Properties props = new Properties();
    String groupId = "testGiganticLargeMessages-" + UUID.randomUUID();
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    // No auto commit
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // Not enough memory to assemble anything
    props.setProperty(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_BUFFER_CAPACITY_CONFIG, "" + (MAX_SEGMENT_SIZE + 1));
    props.setProperty(LiKafkaConsumerConfig.EXCEPTION_ON_MESSAGE_DROPPED_CONFIG, "false");

    LiKafkaConsumer<String, String> tempConsumer = createConsumer(props);
    tempConsumer.assign(tps);

    //traverse entire partition

    int topicSize = interleaved.size();
    long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(120);
    int msgsDelivered = 0;
    while (true) {
      ConsumerRecords<String, String> records = tempConsumer.poll(1000);
      msgsDelivered += records.count();
      long position = tempConsumer.position(tp);
      if (position >= topicSize) {
        break;
      }
      if (System.currentTimeMillis() > timeout) {
        throw new IllegalStateException("unable to consume to  the end of the topic within timeout."
            + " position=" + position + ". end=" + topicSize);
      }
    }

    Assert.assertTrue(msgsDelivered == 0, "no msgs were expected to be delivered. instead got " + msgsDelivered);

    //make sure offsets committed reflect the msgs we've given up on

    tempConsumer.commitSync();
    OffsetAndMetadata committed = tempConsumer.committed(tp);
    Assert.assertEquals(committed.offset(), topicSize); //li consumer would claim to be at end

    Properties vanillaProps = getConsumerProperties(props);
    KafkaConsumer<String, String> vanillaConsumer = new KafkaConsumer<>(vanillaProps);
    vanillaConsumer.assign(tps);
    OffsetAndMetadata vanillaCommitted = vanillaConsumer.committed(tp);
    Assert.assertEquals(vanillaCommitted.offset(), topicSize - 1); //vanilla offset is one before (1 fragment in buffer)
  }

  @Test
  public void testExceptionOnLargeMsgDropped() throws Exception {
    MessageSplitter splitter = new MessageSplitterImpl(MAX_SEGMENT_SIZE,
        new DefaultSegmentSerializer(),
        new UUIDFactory.DefaultUUIDFactory<>());

    String topic = "testExceptionOnLargeMsgDropped";
    createTopic(topic);
    TopicPartition tp = new TopicPartition(topic, 0);
    Collection<TopicPartition> tps = new ArrayList<>(Collections.singletonList(tp));

    //send a gigantic msg

    Producer<byte[], byte[]> producer = createRawProducer();
    // M0, 20 segments
    UUID messageId0 = LiKafkaClientsUtils.randomUUID();
    String message0 = KafkaTestUtils.getRandomString(20 * MAX_SEGMENT_SIZE);
    List<ProducerRecord<byte[], byte[]>> m0Segs = splitter.split(topic, 0, messageId0, message0.getBytes());

    for (ProducerRecord<byte[], byte[]> rec : m0Segs) {
      producer.send(rec).get();
    }

    //consumer has no hope of assembling the msg

    Properties props = new Properties();
    String groupId = "testExceptionOnLargeMsgDropped-" + UUID.randomUUID();
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    // No auto commit
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // Not enough memory to assemble anything
    props.setProperty(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_BUFFER_CAPACITY_CONFIG, "" + (MAX_SEGMENT_SIZE + 1));
    props.setProperty(LiKafkaConsumerConfig.EXCEPTION_ON_MESSAGE_DROPPED_CONFIG, "true");

    LiKafkaConsumer<String, String> tempConsumer = createConsumer(props);
    tempConsumer.assign(tps);

    int topicSize = m0Segs.size();
    long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(120);
    int msgsDelivered = 0;
    while (true) {
      ConsumerRecords<String, String> records;
      try {
        records = tempConsumer.poll(1000);
      } catch (ConsumerRecordsProcessingException expected) {
        Assert.assertEquals(msgsDelivered, 0);
        break;
      }
      msgsDelivered += records.count();
      long position = tempConsumer.position(tp);
      if (System.currentTimeMillis() > timeout) {
        throw new IllegalStateException("unable to consume to  the end of the topic within timeout."
            + " position=" + position + ". end=" + topicSize);
      }
    }
  }

  public static <T> List<T> interleave(
      final List<T> list1,
      final List<T> list2
  ) {
    List<T> result = new ArrayList<T>(list1.size() + list2.size());

    Iterator<T> it1 = list1.iterator();
    Iterator<T> it2 = list2.iterator();
    while (it1.hasNext() || it2.hasNext()) {
      if (it1.hasNext()) {
        result.add(it1.next());
      }
      if (it2.hasNext()) {
        result.add(it2.next());
      }
    }
    return result;
  }

  @DataProvider(name = "offsetResetStrategies")
  public static Object[][] offsetResetStrategies() {
    return new Object[][] {
        { LiOffsetResetStrategy.EARLIEST },
        { LiOffsetResetStrategy.LATEST},
        { LiOffsetResetStrategy.LICLOSEST },
        { LiOffsetResetStrategy.NONE }
    };
  }

  @Test(dataProvider = "offsetResetStrategies")
  public void testOffsetOutOfRangeForStrategy(LiOffsetResetStrategy strategy) throws Exception {
    createTopic(TOPIC1);
    createTopic(TOPIC2);
    produceRecordsWithKafkaProducer();
    Properties props = new Properties();
    props.setProperty("auto.offset.reset", strategy.name());
    props.setProperty("group.id", "testOffsetOutOfRange");

    TopicPartition tp = new TopicPartition(TOPIC1, 0);
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      consumer.assign(Collections.singleton(tp));
      ConsumerRecords<String, String> consumerRecords = ConsumerRecords.empty();
      consumer.seek(tp, 0);
      while (consumerRecords.isEmpty()) {
        consumerRecords = consumer.poll(1000);
      }
      consumer.seek(tp, 100000L);
      assertEquals(consumer.position(tp), 100000L);
      switch (strategy) {
        case EARLIEST:
          long expectedEarliestOffset = consumerRecords.iterator().next().offset();
          consumerRecords = ConsumerRecords.empty();
          while (consumerRecords.isEmpty()) {
            consumerRecords = consumer.poll(1000);
          }
          assertEquals(consumerRecords.iterator().next().offset(), expectedEarliestOffset,
              "The offset should have been reset to the earliest offset");
          break;
        case LATEST:
          consumer.poll(1000);
          long expectedLatestOffset = consumer.endOffsets(Collections.singleton(tp)).get(tp);
          assertEquals(consumer.position(tp), expectedLatestOffset,
              "The offset should have been reset to the latest offset");
          break;
        case LICLOSEST:
          long expectedEndOffset = consumer.endOffsets(Collections.singleton(tp)).get(tp);
          consumer.poll(1000);
          long currentPosition = consumer.position(tp);
          assertTrue(currentPosition == expectedEndOffset);
          break;
        case NONE:
          try {
            consumer.poll(1000);
            fail("OffsetOutOfRangeException should have been thrown.");
          } catch (OffsetOutOfRangeException oore) {
            // Expected
          }
          break;
        default:
          fail("Unknown reset strategy " + strategy);
          break;
      }
    }
  }

  /*
   * This tests verifies that the consumer (with LICLOSEST policy) resets to earliest record in the log when
   * there committed offset is not available. This can happen when a new consumer group is created (bootstrap) or
   * if the committed consumer offset expired. This test reproduces the former case (bootstrap) to assert the behavior.
   */
  @Test
  public void testBootstrapWithLiClosest() throws Exception {
    createTopic(TOPIC1);
    createTopic(TOPIC2);
    produceRecordsWithKafkaProducer();
    Properties props = new Properties();
    props.setProperty("auto.offset.reset", LiOffsetResetStrategy.LICLOSEST.name());
    props.setProperty("group.id", "testBootstrapLICLOSEST");

    LiKafkaConsumer<String, String> consumer = createConsumer(props);
    TopicPartition tp = new TopicPartition(TOPIC1, 0);
    try {
      consumer.assign(Collections.singleton(tp));
      ConsumerRecords<String, String> consumerRecords = ConsumerRecords.empty();
      while (consumerRecords.isEmpty()) {
        consumerRecords = consumer.poll(1000);
      }
      long bootstrappedReset = consumerRecords.iterator().next().offset();
      consumer.seekToBeginning(Collections.singleton(tp));
      consumerRecords = ConsumerRecords.empty();
      while (consumerRecords.isEmpty()) {
        consumerRecords = consumer.poll(1000);
      }
      assertEquals(bootstrappedReset, consumerRecords.iterator().next().offset()); // because we seek to beginning above
    } finally {
      consumer.close();
    }
  }

  /*
   * In order to test consumer fetching from an offset < LSO:
   * - consumer commits initial LSO
   * - waits until log truncation happens by continuously checking for beginningOffsets
   * - Start a new consumer instance and consume from committed offset (which is less than the current LSO)
   * - Verify that the offset reset returns the earliest available record
   */
  @Test
  public void testFallOffStartWithLiClosest() throws Exception {
    createTopic(TOPIC1);
    createTopic(TOPIC2);
    produceRecordsWithKafkaProducer();
    Properties props = new Properties();
    props.setProperty("auto.offset.reset", LiOffsetResetStrategy.LICLOSEST.name());
    props.setProperty("group.id", "testFallOffStart");
    // Disable auto-commit for controlled testing
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    long truncatedStartOffset;
    TopicPartition tp = new TopicPartition(TOPIC1, 0);
    Set<TopicPartition> tpAsCollection = Collections.singleton(tp);
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      consumer.assign(tpAsCollection);
      long initialLso = consumer.beginningOffsets(tpAsCollection).get(tp); //very likely 0, but not guaranteed
      long currentLso = initialLso;
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(initialLso, String.valueOf(consumer.safeOffset(tp)))));
      //wait for broker to truncate data that we have not read (we never called poll())
      long giveUp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
      while (currentLso == initialLso && System.currentTimeMillis() < giveUp) {
        Thread.sleep(5000);
        currentLso = consumer.beginningOffsets(tpAsCollection).get(tp);
        produceRecordsWithKafkaProducer();
      }
      if (currentLso == initialLso) {
        throw new IllegalStateException("nothing was truncated broker-side within timeout. LogStartOffset = " +
            currentLso + " remains the same after " + giveUp + "ms.");
      }
      truncatedStartOffset = currentLso;
    }
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      ConsumerRecords<String, String> consumerRecords = ConsumerRecords.empty();
      consumer.assign(tpAsCollection);
      long giveUp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
      while (consumerRecords.isEmpty() && System.currentTimeMillis() < giveUp) {
        consumerRecords = consumer.poll(Duration.ofMillis(100));
      }
      if (consumerRecords.isEmpty()) {
        throw new IllegalStateException("failed to consume any records within timeout");
      }
      long actualResetOffset = consumerRecords.iterator().next().offset();
      // Record offset may be greater than the earliest log offset if the first record is a large-message
      assertTrue(actualResetOffset >= truncatedStartOffset);
      assertTrue(actualResetOffset < consumer.endOffsets(Collections.singleton(tp)).get(tp));
    }
  }

  /**
   * This method produce a bunch of messages in an interleaved way.
   * @param messages will contain both large message and ordinary messages.
   */
  private void produceMessages(ConcurrentMap<String, String> messages, String topic, boolean useWallClock) throws InterruptedException {
    int sizeBefore = messages.size();

    Properties props = new Properties();
    // Enable large messages.
    props.setProperty("large.message.enabled", "true");
    // Set segment size to 200 so we have many large messages.
    props.setProperty("max.message.segment.size", Integer.toString(MAX_SEGMENT_SIZE));
    props.setProperty("client.id", "testProducer");
    // Set batch size to 1 to make sure each message is a batch so we have a lot interleave messages.
    props.setProperty("batch.size", "1");

    // Create a few producers to make sure we have interleaved large messages.
    List<LiKafkaProducer<String, String>> producers = new ArrayList<>();
    for (int i = 0; i < NUM_PRODUCER; i++) {
      producers.add(createProducer(props));
    }
    // Let each producer have a few user threads sending messages.
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < NUM_PRODUCER; i++) {
      for (int j = 0; j < THREADS_PER_PRODUCER; j++) {
        threads.add(new ProducerThread(producers.get(i), messages, topic, useWallClock));
      }
    }
    // Start user threads.
    for (Thread thread : threads) {
      thread.start();
    }
    // Wait until the user threads finish sending.
    for (Thread thread : threads) {
      thread.join();
    }
    // Close producers.
    for (LiKafkaProducer<String, String> producer : producers) {
      producer.close();
    }

    int sizeAfter = messages.size();
    int produced = sizeAfter - sizeBefore;
    Assert.assertEquals(produced, NUM_PRODUCER * THREADS_PER_PRODUCER * MESSAGE_COUNT);
  }

  private static final class MessageIdComparator implements Comparator<String> {

    @Override
    public int compare(String o1, String o2) {
      String[] parts1 = o1.split("-");
      String[] parts2 = o2.split("-");

      int diff = parts1[0].compareTo(parts2[0]);
      if (diff != 0) {
        return diff;
      }
      diff = Integer.parseInt(parts1[1]) - Integer.parseInt(parts2[1]);
      if (diff != 0) {
        return diff;
      }

      return Integer.parseInt(parts1[2]) - Integer.parseInt(parts2[2]);
    }
  }

  private class ProducerThread extends Thread {
    private final LiKafkaProducer<String, String> _producer;
    private final ConcurrentMap<String, String> _messages;
    private final String _topic;
    private final boolean _useWallClock;

    public ProducerThread(LiKafkaProducer<String, String> producer, ConcurrentMap<String, String> messages,
      String topic, boolean useWallClock) {
      _producer = producer;
      _messages = messages;
      _topic = topic;
      _useWallClock = useWallClock;
    }

    @Override
    public void run() {
      final Set<String> ackedMessages = new HashSet<>();
      for (int i = 0; i < MESSAGE_COUNT; i++) {
        // The message size is set to 100 - 1124, So we should have most of the messages to be large messages
        // while still have some ordinary size messages.
        int messageSize = 100 + _random.nextInt(1024);
        final String uuid = LiKafkaClientsUtils.randomUUID().toString().replace("-", "");
        final String message = uuid + KafkaTestUtils.getRandomString(messageSize);

        long timestamp = _useWallClock ? System.currentTimeMillis() : i;
        _producer.send(new ProducerRecord<String, String>(_topic, null, timestamp, null, message),
            new Callback() {
              @Override
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // The callback should have been invoked only once.
                assertFalse(ackedMessages.contains(uuid));
                if (e == null) {
                  ackedMessages.add(uuid);
                } else {
                  e.printStackTrace();
                }
                assertNull(e);
                String messageId = messageId(recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                _messages.put(messageId, message);
              }
            });
      }
    }
  }

  /**
   * This is a key we use to check what records have been produced.
   */
  private static String messageId(String topic, int partition, long offset) {
    return topic + "-" + partition + "-" + offset;
  }

  private void produceRecordsWithKafkaProducer() {
    produceRecordsWithKafkaProducer(false);
  }

  private void produceRecordsWithKafkaProducer(boolean useWallClock) {
    _messages = new ConcurrentSkipListMap<>();
    _random = new Random(23423423);
    try {
      produceMessages(_messages, TOPIC1, useWallClock);
      produceMessages(_messages, TOPIC2, useWallClock);
    } catch (InterruptedException e) {
      throw new RuntimeException("Message producing phase failed.", e);
    }
  }

  /** Generate the following synthetic messages in order and produce to the same partition.
   * <pre>
   * partition SYNTHETIC_PARTITION_0
   *                              safe offset
   * 0: M0_SEG0 (START)         | 0
   * 1: M1_SEG0 (START)         | 0
   * 2: M2_SEG0 (START) (END)   | 0
   * 3: M3_SEG0 (START)         | 0
   * 4: M1_SEG1(END)            | 0
   * 5: M0_SEG1(END)            | 0
   * 6: M3_SEG1(END)            | 3
   * 7: M4_SEG0 (START) (END)   | 7
   * 8: M5_SEG0 (START)         | 8
   * 9: M5_SEG1 (END)           | 8
   * </pre>
   */
  private void produceSyntheticMessages(String topic) {
    MessageSplitter splitter = new MessageSplitterImpl(MAX_SEGMENT_SIZE,
        new DefaultSegmentSerializer(),
        new UUIDFactory.DefaultUUIDFactory<>());

    Producer<byte[], byte[]> producer = createRawProducer();
    // Prepare messages.
    int messageSize = MAX_SEGMENT_SIZE + MAX_SEGMENT_SIZE / 2;
    // M0, 2 segments
    UUID messageId0 = LiKafkaClientsUtils.randomUUID();
    String message0 = KafkaTestUtils.getRandomString(messageSize);
    List<ProducerRecord<byte[], byte[]>> m0Segs = splitter.split(topic, SYNTHETIC_PARTITION_0, messageId0, message0.getBytes());
    // M1, 2 segments
    UUID messageId1 = LiKafkaClientsUtils.randomUUID();
    String message1 = KafkaTestUtils.getRandomString(messageSize);
    List<ProducerRecord<byte[], byte[]>> m1Segs = splitter.split(topic, SYNTHETIC_PARTITION_0, messageId1, message1.getBytes());
    // M2, 1 segment
    UUID messageId2 = LiKafkaClientsUtils.randomUUID();
    String message2 = KafkaTestUtils.getRandomString(MAX_SEGMENT_SIZE / 2);
    List<ProducerRecord<byte[], byte[]>> m2Segs = splitter.split(topic, SYNTHETIC_PARTITION_0, messageId2, message2.getBytes());
    // M3, 2 segment
    UUID messageId3 = LiKafkaClientsUtils.randomUUID();
    String message3 = KafkaTestUtils.getRandomString(messageSize);
    List<ProducerRecord<byte[], byte[]>> m3Segs = splitter.split(topic, SYNTHETIC_PARTITION_0, messageId3, message3.getBytes());
    // M4, 1 segment
    UUID messageId4 = LiKafkaClientsUtils.randomUUID();
    String message4 = KafkaTestUtils.getExceptionString(MAX_SEGMENT_SIZE / 2);
    List<ProducerRecord<byte[], byte[]>> m4Segs = splitter.split(topic, SYNTHETIC_PARTITION_0, messageId4, message4.getBytes());
    // M5, 2 segments
    UUID messageId5 = LiKafkaClientsUtils.randomUUID();
    String message5 = KafkaTestUtils.getRandomString(messageSize);
    List<ProducerRecord<byte[], byte[]>> m5Segs = splitter.split(topic, SYNTHETIC_PARTITION_0, messageId5, message5.getBytes());


    // Add two more segment to partition SYNTHETIC_PARTITION_1 for corner case test.
    List<ProducerRecord<byte[], byte[]>> m0SegsPartition1 = splitter.split(topic, SYNTHETIC_PARTITION_1, LiKafkaClientsUtils.randomUUID(), message0.getBytes());
    List<ProducerRecord<byte[], byte[]>> m1SegsPartition1 = splitter.split(topic, SYNTHETIC_PARTITION_1, LiKafkaClientsUtils.randomUUID(), message1.getBytes());

    List<Future<RecordMetadata>> futures = new ArrayList<>();
    try {
      futures.add(producer.send(m0Segs.get(0)));
      futures.add(producer.send(m1Segs.get(0)));
      futures.add(producer.send(m2Segs.get(0)));
      futures.add(producer.send(m3Segs.get(0)));
      futures.add(producer.send(m1Segs.get(1)));
      futures.add(producer.send(m0Segs.get(1)));
      futures.add(producer.send(m3Segs.get(1)));
      futures.add(producer.send(m4Segs.get(0)));
      futures.add(producer.send(m5Segs.get(0)));
      futures.add(producer.send(m5Segs.get(1)));

      futures.add(producer.send(m0SegsPartition1.get(0)));
      futures.add(producer.send(m1SegsPartition1.get(0)));

      for (Future<RecordMetadata> future : futures) {
        future.get();
      }

    } catch (Exception e) {
      fail("Produce synthetic data failed.", e);
    }
    producer.close();
  }

  private void createTopic(String topicName) throws Exception {
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, NUM_PARTITIONS, (short) 1))).all().get(1, TimeUnit.MINUTES);
    }
  }

  private class RebalanceTestConsumerThread extends Thread {
    private final LiKafkaConsumer<String, String> _consumer;
    private final int _id;
    private final Map<String, String> _messageUnseen;
    private final TestRebalanceListener _listener;
    private final long quietMillis = TimeUnit.SECONDS.toMillis(10);
    private final AtomicReference<Throwable> _error;

    RebalanceTestConsumerThread(LiKafkaConsumer<String, String> consumer,
                                Map<String, String> messageUnseen,
                                int id, AtomicReference<Throwable> error) {
      super("consumer-thread-" + id);
      _consumer = consumer;
      _id = id;
      _messageUnseen = messageUnseen;
      _listener = new TestRebalanceListener();
      _error = error;
    }

    private void processConsumedRecord(ConsumerRecords<String, String> records) {
      for (ConsumerRecord<String, String> record : records) {
        String messageId = messageId(record.topic(), record.partition(), record.offset());
        String origMessage = _messageUnseen.get(messageId);
        assertEquals(record.value(), origMessage, "Message should be the same. partition = " +
            record.topic() + "-" + record.partition() + ", offset = " + record.offset());
        _messageUnseen.remove(messageId);
      }
    }

    @Override
    public void run() {
      try {
        _consumer.subscribe(Arrays.asList(TOPIC1, TOPIC2), _listener);
        long noActivityDeadline = System.currentTimeMillis() + quietMillis;
        while (!_messageUnseen.isEmpty() && System.currentTimeMillis() < noActivityDeadline && _error.get() == null) {
          int numConsumed = 0;
          while (numConsumed < 150 && !_messageUnseen.isEmpty() && System.currentTimeMillis() < noActivityDeadline) {
            ConsumerRecords<String, String> records = _consumer.poll(10);
            if (records.count() > 0) {
              noActivityDeadline = System.currentTimeMillis() + quietMillis;
            }
            numConsumed += records.count();
            processConsumedRecord(records);
          }

          // Let the consumer to change the subscriptions based on the thread id.
          if (_id % 2 == 0) {
            if (_consumer.subscription().contains(TOPIC2)) {
              _consumer.subscribe(Collections.singleton(TOPIC1), _listener);
            } else {
              _consumer.subscribe(Arrays.asList(TOPIC1, TOPIC2), _listener);
            }
          } else {
            if (_consumer.subscription().contains(TOPIC1)) {
              _consumer.subscribe(Collections.singleton(TOPIC2), _listener);
            } else {
              _consumer.subscribe(Arrays.asList(TOPIC1, TOPIC2), _listener);
            }
          }
          _listener.done = false;
          while (!_messageUnseen.isEmpty() && !_listener.done && _error.get() == null) {
            ConsumerRecords<String, String> records = _consumer.poll(500);
            if (records.count() > 0) {
              noActivityDeadline = System.currentTimeMillis() + quietMillis;
            }
            processConsumedRecord(records);
          }
        }
      } catch (Throwable t) {
        _error.compareAndSet(null, t);
      }
    }

    private class TestRebalanceListener implements ConsumerRebalanceListener {
      public volatile boolean done = false;

      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
        done = true;
      }
    }
  }

}
