/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.largemessage.MessageSplitter;
import com.linkedin.kafka.clients.largemessage.MessageSplitterImpl;
import com.linkedin.kafka.clients.largemessage.errors.OffsetNotTrackedException;
import com.linkedin.kafka.clients.producer.ExtensibleProducerRecord;
import com.linkedin.kafka.clients.producer.LiKafkaProducer;
import com.linkedin.kafka.clients.producer.LiKafkaProducerImpl;
import com.linkedin.kafka.clients.utils.SimplePartitioner;
import com.linkedin.kafka.clients.utils.TestUtils;
import com.linkedin.kafka.clients.utils.UUIDFactory;
import com.linkedin.kafka.clients.utils.UUIDFactoryImpl;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;


/**
 * Integration test for LiKafkaConsumer
 */
public class LiKafkaConsumerIntegrationTest extends AbstractKafkaClientsIntegrationTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaConsumerIntegrationTest.class);

  private final int MESSAGE_COUNT = 1000;
  private Random _random;
  private final String TOPIC1 = "topic1";
  private final String TOPIC2 = "topic2";
  private final int NUM_PRODUCER = 2;
  private final int THREADS_PER_PRODUCER = 2;
  private final int NUM_PARTITIONS = 4;
  private final int MAX_SEGMENT_SIZE = 200;
  private final int SYNTHETIC_PARTITION = 1;
  private Map<String, String> _messages;

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
   * This test tests the seek behavior using the following synthetic data.
   * 0: M0_SEG0
   * 1: M1_SEG0
   * 2: M2_SEG0(END)
   * 3: M3_SEG0
   * 4: M1_SEG1(END)
   * 5: M0_SEG1(END)
   * 6: M3_SEG1(END)
   * 7: M4_SEG0(END)
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
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION);
      consumer.assign(Collections.singleton(tp));

      // Now seek to message offset 0
      consumer.seek(tp, 0);
      verifyMessagesAfterSeek(consumer, Arrays.asList(2L, 4L, 5L, 6L, 7L));

      // Now seek to message offset 1 which is not a message offset. An exception should be thrown because
      // there is no earlier delivered message.
      try {
        consumer.seek(tp, 1);
        fail("Should throw OffsetNotTrackedException");
      } catch (OffsetNotTrackedException onte) {
        // let it go.
      }

      // Now seek to message offset 2
      consumer.seek(tp, 2);
      verifyMessagesAfterSeek(consumer, Arrays.asList(2L, 4L, 5L, 6L, 7L));

      // Now seek to message offset 3, it is a non message offset. The consumer will actually seek to safe offset
      // of 2. m2 should be ignored and m1 should be delivered.
      consumer.seek(tp, 3);
      verifyMessagesAfterSeek(consumer, Arrays.asList(4L, 5L, 6L, 7L));

      // Now seek to message offset 4, m1 should be delivered but m2 should not be delivered.
      consumer.seek(tp, 4);
      verifyMessagesAfterSeek(consumer, Arrays.asList(4L, 5L, 6L, 7L));

      // Now seek to message offset 5, m0 should be delivered but m1 and m2 should not be delivered.
      consumer.seek(tp, 5);
      verifyMessagesAfterSeek(consumer, Arrays.asList(5L, 6L, 7L));

      // Now seek to message offset 6, m3 should be delivered but m2 should not be delivered.
      consumer.seek(tp, 6);
      verifyMessagesAfterSeek(consumer, Arrays.asList(6L, 7L));

      // Now seek to message offset 7, m4 should be delivered but m2 should not be delivered.
      consumer.seek(tp, 7);
      verifyMessagesAfterSeek(consumer, Arrays.asList(7L));
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

  /**
   * This test tests the commit behavior with the following synthetic data:
   * 0: M0_SEG0
   * 1: M1_SEG0
   * 2: M2_SEG0(END)
   * 3: M3_SEG0
   * 4: M1_SEG1(END)
   * 5: M0_SEG1(END)
   * 6: M3_SEG1(END)
   * 7: M4_SEG0(END)
   */
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
    LiKafkaConsumer<String, String> consumer = createConsumer(props);
    try {
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION);
      consumer.assign(Collections.singleton(tp));

      while (consumer.poll(10).isEmpty()) {
        // M2
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(3, ""), "The committed user offset should be 3");
      assertEquals(consumer.committedSafeOffset(tp), 0, "The committed actual offset should be 0");

      while (consumer.poll(10).isEmpty()) {
        // M1
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(5, ""), "The committed user offset should be 5");
      assertEquals(consumer.committedSafeOffset(tp), 0, "The committed actual offset should be 0");

      while (consumer.poll(10).isEmpty()) {
        // M0
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(6, ""), "The committed user offset should be 6");
      assertEquals(consumer.committedSafeOffset(tp), 3, "The committed actual offset should be 3");

      while (consumer.poll(10).isEmpty()) {
        // M3
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(7, ""), "The committed user offset should be 7");
      assertEquals(consumer.committedSafeOffset(tp), 7, "The committed actual offset should be 7");

      while (consumer.poll(10).isEmpty()) {
        // M4
      }
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(8, ""), "The committed user offset should be 8");
      assertEquals(consumer.committedSafeOffset(tp), 8, "The committed actual offset should be 8");

      consumer.close();
      consumer = createConsumer(props);
      consumer.assign(Collections.singleton(tp));
      consumer.seekToCommitted(Collections.singleton(tp));
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(8, "new commit")));
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(8, "new commit"));
      assertEquals(consumer.committedSafeOffset(tp), 8);
    } finally {
      consumer.close();
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
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION);
      consumer.assign(Collections.singleton(tp));
      consumer.poll(1000); // M2
      consumer.commitSync();
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(3, ""), "The committed user offset should be 3");
      assertEquals(consumer.committedSafeOffset(tp), 0, "The committed actual offset should be 0");

      consumer.close();
      consumer = createConsumer(props);
      consumer.assign(Collections.singleton(tp));
      consumer.seekToCommitted(Collections.singleton(tp));
      assertEquals(consumer.position(tp), 0, "The committed safe offset should be 0");

      ConsumerRecords<String, String> records = consumer.poll(1000); // M1

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
      TopicPartition tp = new TopicPartition(topic, SYNTHETIC_PARTITION);
      consumer.assign(Collections.singleton(tp));
      consumer.poll(1000); // M2
      final AtomicBoolean offsetCommitted = new AtomicBoolean(false);
      consumer.commitAsync(new OffsetCommitCallback() {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap, Exception e) {
          assertEquals(topicPartitionOffsetAndMetadataMap.get(tp), new OffsetAndMetadata(3, ""), "The committed user offset should be 3");
          offsetCommitted.set(true);
        }
      });
      while (!offsetCommitted.get()) {
        consumer.poll(10);
      }
      assertEquals(consumer.committed(tp), new OffsetAndMetadata(3, ""), "The committed user offset should be 3");
      assertEquals(consumer.committedSafeOffset(tp), 0, "The committed actual offset should be 0");
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
      records = consumer.poll(1000);
    }

    consumer.assign(Collections.singleton(tp1));
    records = null;
    while (records == null || records.isEmpty()) {
      records = consumer.poll(1000);
    }
    // we should be able to seek on tp 0 after assignment change.
    consumer.assign(Arrays.asList(tp0, tp1));
    assertEquals(consumer.safeOffset(tp0), Long.MAX_VALUE, "The safe offset for " + tp0 + " should be Long.MAX_VALUE now.");
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
        records = consumer.poll(1000);
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
    props.setProperty("max.partition.fetch.bytes", "64000");
    LiKafkaConsumer<String, String> consumer = createConsumer(props);

    try {
      // Subscribe to the partitions.
      consumer.subscribe(Arrays.asList(TOPIC1, TOPIC2));

      // Create a new map to record unseen messages which initially contains all the produced _messages.
      Map<String, String> messagesUnseen = new HashMap<>(_messages);

      long startTime = System.currentTimeMillis();
      int numMessagesConsumed = 0;
      int lastCommitAndResume = 0;
      int numCommitAndResume = 0;
      Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
      ConsumerRecords<String, String> records;
      while (!messagesUnseen.isEmpty() && startTime + 30000 > System.currentTimeMillis()) {
        records = consumer.poll(100);

        for (ConsumerRecord<String, String> record : records) {
          String messageId = record.topic() + "-" + record.partition() + "-" + record.offset();
          // We should not see any duplicate message.
          String origMessage = messagesUnseen.get(messageId);
          assertEquals(record.value(), origMessage, "Message should be the same.");
          messagesUnseen.remove(messageId);
          offsetMap.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
          numMessagesConsumed++;
          // We try to stop and recreate a consumer every 1000 messages and at most stop and resume 4 times.
          if (lastCommitAndResume + (NUM_PRODUCER * THREADS_PER_PRODUCER * MESSAGE_COUNT) / 4 < numMessagesConsumed &&
              numCommitAndResume < 4 && offsetMap.size() == NUM_PARTITIONS) {
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
   * This method produce a bunch of messages in an interleaved way.
   * @param messages will contain both large message and ordinary messages.
   */
  private void produceMessages(Map<String, String> messages, String topic) throws InterruptedException {
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

  private class ProducerThread extends Thread {
    private final LiKafkaProducer<String, String> _producer;
    private final Map<String, String> _messages;
    private final String _topic;

    public ProducerThread(LiKafkaProducer<String, String> producer,
                          Map<String, String> messages,
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
        final String messageId = UUID.randomUUID().toString().replace("-", "");
        final String message = messageId + TestUtils.getRandomString(messageSize);

        _producer.send(new ProducerRecord<>(_topic, message),
            new Callback() {
              @Override
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // The callback should have been invoked only once.
                assertFalse(ackedMessages.contains(messageId));
                if (e == null) {
                  ackedMessages.add(messageId);
                }
                String messageId = messageId(recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                //System.out.println("Adding message: " + messageId);
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
   * 0: M0_SEG0
   * 1: M1_SEG0
   * 2: M2_SEG0(END)
   * 3: M3_SEG0
   * 4: M1_SEG1(END)
   * 5: M0_SEG1(END)
   * 6: M3_SEG1(END)
   * 7: M4_SEG0(END)
   * </pre>
   */
  private void produceSyntheticMessages(String topic) {
    UUIDFactory uuidFactory = new UUIDFactoryImpl();
    int partition = SYNTHETIC_PARTITION;
    SimplePartitioner simplePartitioner = new SimplePartitioner() {
      @Override
      public int partition(String topic) {
        throw new IllegalStateException("Partitioner should not have been called.");
      }
    };

    MessageSplitter splitter = new MessageSplitterImpl(MAX_SEGMENT_SIZE, uuidFactory, simplePartitioner);

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

    try {
      //This produces the sequence of records: 0, 1, 2, 3, 1, 0, 3, 4
      producer.send(m0Segs.next()).get();
      producer.send(m1Segs.next()).get();
      producer.send(m2Segs.next()).get();
      producer.send(m3Segs.next()).get();
      producer.send(m1Segs.next()).get();
      producer.send(m0Segs.next()).get();
      producer.send(m3Segs.next()).get();
      producer.send(m4Segs.next()).get();
    } catch (Exception e) {
      fail("Produce synthetic data failed.", e);
    }
    producer.close();
    LOG.info("Producing synthetic data on topic \"" + topic + " is complete.");
  }

  private Collection<ProducerRecord<byte[], byte[]>> createSerializedProducerRecord(int messageId,
      int messageSize, MessageSplitter splitter, String topic, int partition) {

    String message0 = TestUtils.getRandomString(messageSize);
    ExtensibleProducerRecord<byte[], byte[]> originalRecord0 =
        new ExtensibleProducerRecord<>(topic, partition, null /* timestamp */, new byte[] { (byte)  messageId},  message0.getBytes());

    Collection<ExtensibleProducerRecord<byte[], byte[]>> xRecords = splitter.split(originalRecord0);
    List<ProducerRecord<byte[], byte[]>> producerRecords = new ArrayList<>();
    for (ExtensibleProducerRecord<byte[], byte[]> xRecord : xRecords) {
      producerRecords.add(LiKafkaProducerImpl.serializeWithHeaders(xRecord));
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
        System.out.println("Removing seen message \"" + messageId + "\".");
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
