/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.largemessage.MessageSplitter;
import com.linkedin.kafka.clients.largemessage.MessageSplitterImpl;

import com.linkedin.kafka.clients.producer.ExtensibleProducerRecord;

import com.linkedin.kafka.clients.producer.LiKafkaProducer;
import com.linkedin.kafka.clients.producer.LiKafkaProducerImpl;
import com.linkedin.kafka.clients.headers.DefaultHeaderSerializer;
import com.linkedin.kafka.clients.headers.HeaderSerializer;
import com.linkedin.kafka.clients.utils.TestUtils;
import com.linkedin.kafka.clients.utils.UUIDFactory;
import com.linkedin.kafka.clients.utils.UUIDFactoryImpl;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;


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
  private final int MAX_SEGMENT_SIZE = 200;
  private final int SYNTHETIC_PARTITION_0 = 0;
  private final int SYNTHETIC_PARTITION_1 = 1;
  private ConcurrentMap<String, String> _messages;

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();
    props.setProperty(KafkaConfig.NumPartitionsProp(), Integer.toString(NUM_PARTITIONS));
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

  /**
   * This test tests the seek behavior using the synthetic data.
   */
  @Test
  public void testSeek() {
    String topic = "testSeek";
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
  public void testCommit() {
    String topic = "testCommit";
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testCommit");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    LiKafkaConsumer<String, String> consumer = createConsumer(props);
    try {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      TopicPartition tp1 = new TopicPartition(topic, SYNTHETIC_PARTITION_1);
      consumer.assign(Arrays.asList(tp));

      while (consumer.poll(10).isEmpty()) {
        //M2
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(3, ""), "The committed user offset should be 3");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 0, "The committed safe offset should be 0");

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

      consumer.assign(Collections.singleton(tp1));

      while (consumer.poll(10).isEmpty()) {
        // M0, partition 1
      }
      consumer.commitSync();
      assertEquals(consumer.position(tp1), 2L, "The position of partition 1 should be 2.");
      assertEquals(consumer.committedSafeOffset(tp1).longValue(), 2L, "The committed safe offset for partition 1 should be 0.");
      assertEquals(consumer.committed(tp1).offset(), 2L, "The committed offset for partition 1 should be 2.");

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
  public void testSeekToBeginningAndEnd() {
    String topic = "testSeekToBeginningAndEnd";
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
   * This test tests the seekToCommitted() behavior with synthetic data.
   */
  @Test
  public void testSeekToCommitted() {
    String topic = "testSeekToCommitted";
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
      assertEquals(consumer.poll(5000).count(), 1, "Should have consumed 1 message"); // M2
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(3, ""), "The committed user offset should be 3");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 0, "The committed actual offset should be 0");

      consumer.close();
      consumer = createConsumer(props);
      consumer.assign(Collections.singleton(tp));
      // This should work the same as seekToCommitted.
      consumer.seek(tp, consumer.committed(tp).offset());
      assertEquals(consumer.position(tp), 0, "The committed safe offset should be 0");

      ConsumerRecords<String, String> records = consumer.poll(5000); // M1

      assertEquals(records.count(), 1, "There should be only one record.");
      assertEquals(records.iterator().next().offset(), 4, "The message offset should be 4");

    } finally {
      consumer.close();
    }
  }

  @Test
  public void testOffsetCommitCallback() {
    String topic = "testOffsetCommitCallback";
    produceSyntheticMessages(topic);
    Properties props = new Properties();
    // All the consumers should have the same group id.
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testOffsetCommitCallback");
    // Make sure we start to consume from the beginning.
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Only fetch one record at a time.
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    try (LiKafkaConsumer<String, String> consumer = createConsumer(props)) {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION_0);
      consumer.assign(Collections.singleton(tp));
      consumer.poll(5000); // M2
      final AtomicBoolean offsetCommitted = new AtomicBoolean(false);
      OffsetCommitCallback commitCallback = new OffsetCommitCallback() {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap, Exception e) {
          offsetCommitted.set(true);
        }
      };
      consumer.commitAsync(commitCallback);
      while (!offsetCommitted.get()) {
        consumer.poll(10);
      }
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(3, ""), "The committed user offset should be 3");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 0, "The committed actual offset should be 0");

      offsetCommitted.set(false);
      Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
      offsetMap.put(tp, new OffsetAndMetadata(0));
      consumer.commitAsync(offsetMap, commitCallback);
      while (!offsetCommitted.get()) {
        consumer.poll(10);
      }
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(0, ""), "The committed user offset should be 0");
      assertEquals(consumer.committedSafeOffset(tp).longValue(), 0, "The committed actual offset should be 0");
    }
  }

  @Test
  public void testCommittedOnOffsetsCommittedByRawConsumer() {
    String topic = "testCommittedOnOffsetsCommittedByRawConsumer";
    TopicPartition tp = new TopicPartition(topic, 0);
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
  public void testSeekAfterAssignmentChange() {
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
  public void testUnsubscribe() {
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

  /**
   * This is an aggressive rebalance test.
   * There are two consumers in the same group consuming from two topics.
   * The two topics contains mostly large messages.
   * The two consumers will change their subscription frequently to trigger rebalances.
   * The test makes sure that all the messages are consumed exactly once.
   */
  @Test
  public void testRebalance() {
    produceRecordsWithKafkaProducer();
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

    props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
    LiKafkaConsumer<String, String> consumer0 = createConsumer(props);

    props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer1");
    LiKafkaConsumer<String, String> consumer1 = createConsumer(props);

    final Map<String, String> messageUnseen = new ConcurrentSkipListMap<>(_messages);

    Thread thread0 = new RebalanceTestConsumerThread(consumer0, messageUnseen, 0);
    Thread thread1 = new RebalanceTestConsumerThread(consumer1, messageUnseen, 1);

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
    assertEquals(messageUnseen.size(), 0, "Messages unseen: " + messageUnseen.keySet());
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
  public void testCommitAndResume() throws InterruptedException {
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
      while (!messagesUnseen.isEmpty() && startTime + 30000 > System.currentTimeMillis()) {
        records = consumer.poll(100);

        for (ConsumerRecord<String, String> record : records) {
          String messageId = messageId(record.topic(), record.partition(), record.offset());
          // We should not see any duplicate message.
          String origMessage = messagesUnseen.get(messageId);
          assertEquals(record.value(), origMessage, "Message with id \"" + messageId + "\" should be the same.");
          messagesUnseen.remove(messageId);
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
  public void testSearchOffsetByTimestamp() {
    Properties props = new Properties();
    // Make sure we start to consume from the beginning.
    props.setProperty("auto.offset.reset", "earliest");
    // All the consumers should have the same group id.
    props.setProperty("group.id", "testSearchOffsetByTimestamp");
    props.setProperty("enable.auto.commit", "false");
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

  @Test
  public void testOffsetOutOfRange() {
    for (OffsetResetStrategy strategy : OffsetResetStrategy.values()) {
      testOffsetOutOfRangeForStrategy(strategy);
    }
  }

  private void testOffsetOutOfRangeForStrategy(OffsetResetStrategy strategy) {
    produceRecordsWithKafkaProducer();
    Properties props = new Properties();
    // Make sure we start to consume from the beginning.
    props.setProperty("auto.offset.reset", strategy.name());
    // All the consumers should have the same group id.
    props.setProperty("group.id", "testOffsetOutOfRange");

    LiKafkaConsumer<String, String> consumer = createConsumer(props);
    TopicPartition tp = new TopicPartition(TOPIC1, 0);
    try {
      consumer.assign(Collections.singleton(tp));
      ConsumerRecords<String, String> consumerRecords = ConsumerRecords.empty();
      consumer.seek(tp, 0);
      while (consumerRecords.isEmpty()) {
        consumerRecords = consumer.poll(1000);
      }
      consumer.seek(tp, 100000L);
      assertEquals(consumer.position(tp), 100000L);
      if (strategy == OffsetResetStrategy.EARLIEST) {
        long expectedOffset = consumerRecords.iterator().next().offset();
        consumerRecords = ConsumerRecords.empty();
        while (consumerRecords.isEmpty()) {
          consumerRecords = consumer.poll(1000);
        }
        assertEquals(consumerRecords.iterator().next().offset(), expectedOffset,
                     "The offset should have been reset to the earliest offset");
      } else if (strategy == OffsetResetStrategy.LATEST) {
        consumer.poll(1000);
        long expectedOffset = consumer.endOffsets(Collections.singleton(tp)).get(tp);
        assertEquals(consumer.position(tp), expectedOffset, "The offset should have been reset to the latest offset");
      } else {
        consumer.poll(1000);
        fail("OffsetOutOfRangeException should have been thrown.");
      }
    } catch (OffsetOutOfRangeException ooore) {
      if (strategy != OffsetResetStrategy.NONE) {
        fail("Should not have thrown OffsetOutOfRangeException.");
      }
    } finally {
      consumer.close();
    }
  }

  /**
   * This method produce a bunch of messages in an interleaved way.
   * @param messages will contain both large message and ordinary messages.
   */
  private void produceMessages(ConcurrentMap<String, String> messages, String topic) throws InterruptedException {
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
        threads.add(new ProducerThread(producers.get(i), messages, topic));
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

    public ProducerThread(LiKafkaProducer<String, String> producer,
                          ConcurrentMap<String, String> messages,
                          String topic) {
      _producer = producer;
      _messages = messages;
      _topic = topic;
    }

    @Override
    public void run() {
      final Set<String> ackedMessages = new HashSet<>();
      for (int i = 0; i < MESSAGE_COUNT; i++) {
        // The message size is set to 100 - 1124, So we should have most of the messages to be large messages
        // while still have some ordinary size messages.
        int messageSize = 100 + _random.nextInt(1024);
        final String uuid = UUID.randomUUID().toString().replace("-", "");
        final String message = uuid + TestUtils.getRandomString(messageSize);

        _producer.send(new ProducerRecord<String, String>(_topic, null, (long) i, null, message),
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
    _messages = new ConcurrentSkipListMap<>();
    _random = new Random(23423423);
    try {
      produceMessages(_messages, TOPIC1);
      produceMessages(_messages, TOPIC2);
    } catch (InterruptedException e) {
      throw new RuntimeException("Message producing phase failed.", e);
    }
  }

  /** Generate the following synthetic messages in order and produce to the same partition.
   * <pre>
   * partition SYNTHETIC_PARTITION_0
   * 0: M0_SEG0 (START)
   * 1: M1_SEG0 (START)
   * 2: M2_SEG0 (START) (END)
   * 3: M3_SEG0 (START)
   * 4: M1_SEG1(END)
   * 5: M0_SEG1(END)
   * 6: M3_SEG1(END)
   * 7: M4_SEG0 (START) (END)
   * 8: M5_SEG0 (START) (END)
   * 9: M5_SEG1 (END)
   * partition SYNTHETIC_PARTITION_1
   * 0: M0_SEG0 (START)
   * 1: M0_SEG1 (END)
   * </pre>
   */
  private void produceSyntheticMessages(String topic) {
    UUIDFactory uuidFactory = new UUIDFactoryImpl();
    int partition = SYNTHETIC_PARTITION_0;

    MessageSplitter splitter = new MessageSplitterImpl(MAX_SEGMENT_SIZE, uuidFactory);

    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    Producer<byte[], byte[]> producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
    // Prepare messages.
    int twoSegmentSize = MAX_SEGMENT_SIZE + MAX_SEGMENT_SIZE / 2;
    int oneSegmentSize = MAX_SEGMENT_SIZE / 2;
    // M0, 2 segments
    Iterator<ProducerRecord<byte[], byte[]>> m0Segs =
        createSerializedProducerRecord(0, twoSegmentSize, splitter, topic, partition).iterator();

    // M1, 2 segments
    Iterator<ProducerRecord<byte[], byte[]>> m1Segs =
        createSerializedProducerRecord(1, twoSegmentSize, splitter, topic, partition).iterator();
    // M2, 1 segment
    Iterator<ProducerRecord<byte[], byte[]>> m2Segs =
        createSerializedProducerRecord(2, oneSegmentSize, splitter, topic, partition).iterator();
    // M3, 2 segment
    Iterator<ProducerRecord<byte[], byte[]>> m3Segs =
        createSerializedProducerRecord(3, twoSegmentSize, splitter, topic, partition).iterator();
    // M4, 1 segment
    Iterator<ProducerRecord<byte[], byte[]>> m4Segs =
        createSerializedProducerRecord(4, oneSegmentSize, splitter, topic, partition).iterator();

    // M5, 2 segments
    Iterator<ProducerRecord<byte[], byte[]>> m5Segs =
      createSerializedProducerRecord(5, twoSegmentSize, splitter, topic, SYNTHETIC_PARTITION_0).iterator();

    // Add two more segment to partition SYNTHETIC_PARTITION_1 for corner case test.
    Iterator<ProducerRecord<byte[], byte[]>> m0SegsPartition1 =
      createSerializedProducerRecord(0, twoSegmentSize, splitter, topic, SYNTHETIC_PARTITION_1).iterator();

    try {
      producer.send(m0Segs.next()).get();
      producer.send(m1Segs.next()).get();
      producer.send(m2Segs.next()).get();
      producer.send(m3Segs.next()).get();
      producer.send(m1Segs.next()).get();
      producer.send(m0Segs.next()).get();
      producer.send(m3Segs.next()).get();
      producer.send(m4Segs.next()).get();
      producer.send(m5Segs.next()).get();
      producer.send(m5Segs.next()).get();
      producer.send(m0SegsPartition1.next()).get();
      producer.send(m0SegsPartition1.next()).get();
    } catch (Exception e) {
      fail("Produce synthetic data failed.", e);
    }
    producer.close();
  }

  private Collection<ProducerRecord<byte[], byte[]>> createSerializedProducerRecord(int messageId,
      int messageSize, MessageSplitter splitter, String topic, int partition) {

    String message0 = TestUtils.getRandomString(messageSize);
    ExtensibleProducerRecord<byte[], byte[]> originalRecord0 =
        new ExtensibleProducerRecord<>(topic, partition, null /* timestamp */, new byte[] { (byte)  messageId},  message0.getBytes());

    Collection<ExtensibleProducerRecord<byte[], byte[]>> xRecords = splitter.split(originalRecord0);
    List<ProducerRecord<byte[], byte[]>> producerRecords = new ArrayList<>();
    HeaderSerializer headerParser = new DefaultHeaderSerializer();
    for (ExtensibleProducerRecord<byte[], byte[]> xRecord : xRecords) {
      producerRecords.add(LiKafkaProducerImpl.serializeWithHeaders(xRecord, headerParser));
    }
    return producerRecords;
  }


  private class RebalanceTestConsumerThread extends Thread {
    private final LiKafkaConsumer<String, String> _consumer;
    private final int _id;
    private final Map<String, String> _messageUnseen;
    private final TestRebalanceListener _listener;

    RebalanceTestConsumerThread(LiKafkaConsumer<String, String> consumer,
                                Map<String, String> messageUnseen,
                                int id) {
      super("consumer-thread-" + id);
      _consumer = consumer;
      _id = id;
      _messageUnseen = messageUnseen;
      _listener = new TestRebalanceListener();
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
        long startMs = System.currentTimeMillis();
        while (!_messageUnseen.isEmpty() && System.currentTimeMillis() - startMs < 30000) {
          int numConsumed = 0;
          while (numConsumed < 150 && !_messageUnseen.isEmpty() && System.currentTimeMillis() - startMs < 30000) {
            ConsumerRecords<String, String> records = _consumer.poll(10);
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
          while (!_messageUnseen.isEmpty() && !_listener.done) {
            processConsumedRecord(_consumer.poll(10));
          }
        }
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }

    private class TestRebalanceListener implements ConsumerRebalanceListener {
      public boolean done = false;

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
