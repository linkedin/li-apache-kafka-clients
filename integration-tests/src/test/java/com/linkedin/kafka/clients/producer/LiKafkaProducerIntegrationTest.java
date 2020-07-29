/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.consumer.LiKafkaConsumer;
import com.linkedin.kafka.clients.largemessage.errors.SkippableException;
import com.linkedin.kafka.clients.utils.Constants;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.PrimitiveEncoderDecoder;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import java.time.Duration;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


public class LiKafkaProducerIntegrationTest extends AbstractKafkaClientsIntegrationTestHarness {
  // Runs testing against running kafka host
  private static final int RECORD_COUNT = 1000;

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
   * This test pushes test data into a temporary topic to a particular broker, and
   * verifies all data are sent and can be consumed correctly.
   */
  @Test
  public void testSend() throws Exception {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    String tempTopic = "testTopic" + new Random().nextInt(1000000);
    createTopic(tempTopic);
    try (LiKafkaProducer<String, String> producer = createProducer(props)) {
      for (int i = 0; i < RECORD_COUNT; ++i) {
        String value = Integer.toString(i);
        producer.send(new ProducerRecord<>(tempTopic, value));
      }
    }

    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    int messageCount = 0;
    try (LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps)) {
      consumer.subscribe(Collections.singleton(tempTopic));
      BitSet counts = new BitSet(RECORD_COUNT);
      long startMs = System.currentTimeMillis();
      while (messageCount < RECORD_COUNT && System.currentTimeMillis() < startMs + 30000) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
          int index = Integer.parseInt(record.value());
          counts.set(index);
          messageCount++;
        }
      }
    }
    assertEquals(RECORD_COUNT, messageCount);
  }

  @Test
  public void testTimestampHeader() throws Exception {
    long startTime = System.currentTimeMillis();
    Properties props = new Properties();
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    String levelOneTopic = "levelOneTopic" + new Random().nextInt(1000000);
    String levelTwoTopic = "levelTwoTopic" + new Random().nextInt(1000000);
    createTopic(levelOneTopic);
    createTopic(levelTwoTopic);
    try (LiKafkaProducer<String, String> producer = createProducer(props)) {
      for (int i = 0; i < RECORD_COUNT; ++i) {
        String value = Integer.toString(i);
        producer.send(new ProducerRecord<>(levelOneTopic, 0, startTime, null, value));
      }

      Properties consumerProps = new Properties();
      consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      try (LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps)) {
        int messageCount = 0;
        consumer.subscribe(Collections.singleton(levelOneTopic));
        BitSet counts = new BitSet(RECORD_COUNT);
        long startMs = System.currentTimeMillis();
        while (messageCount < RECORD_COUNT && System.currentTimeMillis() < startMs + 30000) {
          ConsumerRecords<String, String> records = consumer.poll(100);
          for (ConsumerRecord<String, String> record : records) {
            Map<String, byte[]> headers = LiKafkaClientsUtils.fetchSpecialHeaders(record.headers());
            assertTrue(headers.containsKey(Constants.TIMESTAMP_HEADER));
            long eventTimestamp = PrimitiveEncoderDecoder.decodeLong(headers.get(Constants.TIMESTAMP_HEADER), 0);
            assertEquals(eventTimestamp, startTime);

            int index = Integer.parseInt(record.value());
            counts.set(index);
            messageCount++;
            producer.send(new ProducerRecord<>(levelTwoTopic, 0, record.key(), record.value(), record.headers()));
          }
        }

        assertEquals(RECORD_COUNT, messageCount);
      }

      try (LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps)) {
        int messageCount = 0;
        consumer.subscribe(Collections.singleton(levelTwoTopic));

        BitSet counts = new BitSet(RECORD_COUNT);
        long startMs = System.currentTimeMillis();
        while (messageCount < RECORD_COUNT && System.currentTimeMillis() < startMs + 30000) {
          ConsumerRecords<String, String> records = consumer.poll(100);
          for (ConsumerRecord<String, String> record : records) {
            Map<String, byte[]> headers = LiKafkaClientsUtils.fetchSpecialHeaders(record.headers());
            assertTrue(headers.containsKey(Constants.TIMESTAMP_HEADER));
            long eventTimestamp = PrimitiveEncoderDecoder.decodeLong(headers.get(Constants.TIMESTAMP_HEADER), 0);
            assertEquals(eventTimestamp, startTime);

            int index = Integer.parseInt(record.value());
            counts.set(index);
            messageCount++;
          }
        }

        assertEquals(RECORD_COUNT, messageCount);
      }
    }
  }

  @Test
  public void testNullValue() throws Exception {
    String topic = "testNullValue";
    createTopic(topic);
    try (LiKafkaProducer<String, String> producer = createProducer(null)) {
      producer.send(new ProducerRecord<>(topic, "key", null));
    }
    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps)) {
      consumer.subscribe(Collections.singleton(topic));
      long startMs = System.currentTimeMillis();
      ConsumerRecords<String, String> records = ConsumerRecords.empty();
      while (records.isEmpty() && System.currentTimeMillis() < startMs + 60000) {
        records = consumer.poll(Duration.ofMillis(100));
      }
      assertEquals(1, records.count());
      ConsumerRecord<String, String> record = records.iterator().next();
      assertEquals("key", record.key());
      assertNull(record.value());
    }
  }

  @Test
  public void testZeroLengthValue() throws Exception {
    String topic = "testZeroLengthValue";
    createTopic(topic);

    Properties producerPropertyOverrides = new Properties();
    producerPropertyOverrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    //this is actually Producer<String, byte[]>
    try (LiKafkaProducer producer = createProducer(producerPropertyOverrides)) {
      //noinspection unchecked
      producer.send(new ProducerRecord<>(topic, "key", new byte[0])).get();
    }
    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    try (LiKafkaConsumer consumer = createConsumer(consumerProps)) {
      consumer.subscribe(Collections.singleton(topic));
      long startMs = System.currentTimeMillis();
      ConsumerRecords records = ConsumerRecords.empty();
      while (records.isEmpty() && System.currentTimeMillis() < startMs + 30000) {
        records = consumer.poll(100);
      }
      assertEquals(1, records.count());
      ConsumerRecord record = (ConsumerRecord) records.iterator().next();
      assertEquals("key", record.key());
      assertEquals(((byte[]) record.value()).length, 0);
    }
  }

  /**
   * This test produces test data into a temporary topic to a particular broker with a non-deseriable value
   * verifies producer.send() will throw exception if and only if SKIP_RECORD_ON_SKIPPABLE_EXCEPTION_CONFIG is false
   */
  @Test
  public void testSerializationException() throws Exception {

    Serializer<String> stringSerializer = new StringSerializer();
    Serializer<String> errorThrowingSerializer = new Serializer<String>() {
      @Override
      public void configure(Map<String, ?> configs, boolean isKey) {

      }

      @Override
      public byte[] serialize(String topic, String value) {
        if (value.equals("ErrorBytes")) {
          throw new SkippableException();
        }
        return stringSerializer.serialize(topic, value);
      }

      @Override
      public void close() {

      }
    };

    Properties props = getProducerProperties(null);
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    String tempTopic = "testTopic" + new Random().nextInt(1000000);
    createTopic(tempTopic);
    try (LiKafkaProducer<String, String> producer = new LiKafkaProducerImpl<>(props, stringSerializer, errorThrowingSerializer, null, null)) {
      producer.send(new ProducerRecord<>(tempTopic, "ErrorBytes"));
      producer.send(new ProducerRecord<>(tempTopic, "value"));
    }

    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    int messageCount = 0;
    try (LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps)) {
      consumer.subscribe(Collections.singleton(tempTopic));
      long startMs = System.currentTimeMillis();
      while (messageCount < 1 && System.currentTimeMillis() < startMs + 30000) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
          assertEquals("value", record.value());
          messageCount++;
        }
      }
    }
    assertEquals(1, messageCount);
  }

  @Test
  public void testCustomPartitioningOfLargeMessage() throws Exception {
    long seed = System.currentTimeMillis();
    Random random = new Random(seed);
    int numPartitions = 2 + random.nextInt(19); // [2-20]
    String topic = "testCustomPartitioner-" + UUID.randomUUID();

    AdminClient adminClient = createRawAdminClient(null);
    CreateTopicsResult createTopicResult = adminClient.createTopics(Collections.singletonList(new NewTopic(topic, numPartitions, (short) 1)));
    createTopicResult.all().get(1, TimeUnit.MINUTES);
    TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(topic)).all().get(1, TimeUnit.MINUTES).get(topic);
    Assert.assertEquals(topicDescription.partitions().size(), numPartitions, "expected topic to have "
        + numPartitions + " partitions. instead got " + topicDescription.partitions() + ". seed is " + seed);

    Properties producerPropertyOverrides = new Properties();
    producerPropertyOverrides.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getCanonicalName());
    producerPropertyOverrides.put(LiKafkaProducerConfig.LARGE_MESSAGE_ENABLED_CONFIG, "true");

    int msgSize = (int) (1.5 * DEFAULT_MAX_SEGMENT_BYTES);
    int numExpectedRecords = (int) Math.ceil(((double) msgSize) / DEFAULT_MAX_SEGMENT_BYTES);
    String randomLargeString = RandomStringUtils.randomAlphanumeric(msgSize);

    try (LiKafkaProducer<String, String> producer =  createProducer(producerPropertyOverrides)) {
      //no explicit partition set, timestamp = now
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, System.currentTimeMillis(), "key", randomLargeString);
      producer.send(record).get();
    }

    List<TopicPartition> tps = topicDescription.partitions().stream()
        .map(tpi -> new TopicPartition(topic, tpi.partition()))
        .collect(Collectors.toList());

    try (Consumer<byte[], byte[]> consumer = createRawConsumer()) {
      Map<Integer, Integer> partToCount = new HashMap<>(); //histogram of partition to how many records seen on it
      int recordsRead = 0;
      consumer.assign(tps);
      consumer.seekToBeginning(tps);
      long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
      while (System.currentTimeMillis() < deadline && recordsRead < numExpectedRecords) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
        recordsRead += records.count();
        for (ConsumerRecord<byte[], byte[]> rec : records) {
          partToCount.compute(rec.partition(), (k, v) -> {
            if (v == null) {
              return 1;
            }
            return v + 1;
          });
        }
      }
      Assert.assertEquals(recordsRead, numExpectedRecords, "only read " + recordsRead + " within timeout. seed is " + seed);
      Assert.assertEquals(partToCount.size(), 1, "segments should all land in the same partition. seed is " + seed);
      Assert.assertEquals(partToCount.keySet().iterator().next().intValue(), 0, "payloads should have landed in partition 0. seed is " + seed);
      Assert.assertTrue(partToCount.values().iterator().next() > 1, "expected to have more than a single segment. seed is " + seed);
    }

    try (LiKafkaConsumer<String, String> consumer = createConsumer(null)) {
      consumer.assign(tps);
      consumer.seekToBeginning(tps);

      long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
      ConsumerRecord<String, String> assembled = null;
      while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        if (records.count() == 0) {
          continue;
        }
        assembled = records.iterator().next();
        break;
      }
      Assert.assertNotNull(assembled, "unable to read assembled record within timeout. seed is " + seed);
      Assert.assertEquals(randomLargeString, assembled.value(), "assembled payload expected to match original. seed is " + seed);
    }
  }

  private void createTopic(String topicName) throws Exception {
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, 2, (short) 1))).all().get(1, TimeUnit.MINUTES);
    }
  }
}

