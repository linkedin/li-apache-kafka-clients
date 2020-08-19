/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.common.LargeMessageHeaderValue;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumer;
import com.linkedin.kafka.clients.producer.LiKafkaProducer;
import com.linkedin.kafka.clients.producer.LiKafkaProducerConfig;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import com.linkedin.kafka.clients.utils.tests.KafkaTestUtils;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.linkedin.kafka.clients.producer.LiKafkaProducerConfig.LARGE_MESSAGE_ENABLED_CONFIG;
import static com.linkedin.kafka.clients.producer.LiKafkaProducerConfig.LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_CONFIG;
import static com.linkedin.kafka.clients.producer.LiKafkaProducerConfig.MAX_MESSAGE_SEGMENT_BYTES_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.testng.Assert.*;


/**
 * The integration test for large message.
 */
public class LargeMessageIntegrationTest extends AbstractKafkaClientsIntegrationTestHarness {
  private static final String TOPIC = "TestLargeMessage";
  private static final int UUID_LENGTH = 16;
  private static final int NUM_PARTITIONS = 4;

  @Override
  public int clusterSize() {
    return 1;
  }

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();
    props.setProperty("num.partitions", Integer.toString(NUM_PARTITIONS));
    return props;
  }

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
  public void testAlwaysUseLargeMessageEnvelope() throws Exception {
    //create the test topic
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1))).all().get(1, TimeUnit.MINUTES);
    }

    {
      long startTime = System.currentTimeMillis();
      Properties props = new Properties();
      props.setProperty(LARGE_MESSAGE_ENABLED_CONFIG, "true");
      props.setProperty(LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_CONFIG, "true");
      props.setProperty(MAX_MESSAGE_SEGMENT_BYTES_CONFIG, "200");
      props.setProperty(CLIENT_ID_CONFIG, "testProducer");
      LiKafkaProducer<String, String> largeMessageProducer = createProducer(props);

      // This is how large we expect the final message to be, including the version byte, checksum, segment info and
      // the user payload itself.
      final int expectedProducedMessageSize =
        + Byte.BYTES
        + Integer.BYTES
        + LargeMessageSegment.SEGMENT_INFO_OVERHEAD
        + "hello".length();

      largeMessageProducer.send(new ProducerRecord<>(TOPIC, "hello"), (recordMetadata, e) -> {
        assertEquals(recordMetadata.serializedValueSize(), expectedProducedMessageSize);
      });
      largeMessageProducer.close();
    }
  }

  @Test
  public void testLargeMessage() throws Exception {
    //create the test topic
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1))).all().get(1, TimeUnit.MINUTES);
    }

    long startTime = System.currentTimeMillis();
    Properties props = new Properties();
    props.setProperty("large.message.enabled", "true");
    props.setProperty("max.message.segment.size", "200");
    props.setProperty("client.id", "testProducer");
    LiKafkaProducer<String, String> largeMessageProducer = createProducer(props);
    Properties consumerProps = buildConsumerProps();
    consumerProps.setProperty("auto.offset.reset", "earliest");
    LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps);

        /* The test will send 100 different large messages to broker, consume from broker and verify the message contents.
           Here for simplicity we use a large message segment as a large message, and chunk this */
    Map<String, String> messages = new HashMap<>();
    int numberOfLargeMessages = 100;
    int largeMessageSize = 1000;
    final Set<String> ackedMessages = new HashSet<>();
    // Produce large messages.
    for (int i = 0; i < numberOfLargeMessages; i++) {
      final String messageId = LiKafkaClientsUtils.randomUUID().toString().replace("-", "");
      String message = messageId + KafkaTestUtils.getRandomString(largeMessageSize);
      messages.put(messageId, message);
      largeMessageProducer.send(new ProducerRecord<>(TOPIC, message), (recordMetadata, e) -> {
        // The callback should have been invoked only once.
        assertFalse(ackedMessages.contains(messageId));
        if (e == null) {
          ackedMessages.add(messageId);
        }
      });
    }
    largeMessageProducer.close();
    // All messages should have been sent.
    assertEquals(ackedMessages.size(), messages.size());

    // Consume and verify the large messages
    List<TopicPartition> partitions = new ArrayList<>();
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      partitions.add(new TopicPartition(TOPIC, i));
    }
    consumer.assign(partitions);

    ConsumerRecords<String, String> records = null;
    boolean consumptionStarted = false;
    while (!consumptionStarted || records.count() > 0) {
      records = consumer.poll(10);
      if (records.count() > 0) {
        consumptionStarted = true;
      }
      for (ConsumerRecord<String, String> consumerRecord : records) {
        // Verify headers
        Long eventTimestamp = LiKafkaClientsUtils.fetchTimestampHeader(consumerRecord.headers());
        assertNotNull(eventTimestamp);
        assertTrue(eventTimestamp >= startTime && eventTimestamp <= System.currentTimeMillis());
        LargeMessageHeaderValue largeMessageHeaderValue = LiKafkaClientsUtils.fetchLargeMessageHeader(consumerRecord.headers());
        assertEquals(largeMessageHeaderValue.getSegmentNumber(), -1);
        assertEquals(largeMessageHeaderValue.getNumberOfSegments(), 6);
        assertEquals(largeMessageHeaderValue.getType(), LargeMessageHeaderValue.LEGACY_V2);

        String messageId = consumerRecord.value().substring(0, 32);
        String origMessage = messages.get(messageId);
        assertEquals(consumerRecord.value(), origMessage, "Messages should be the same");
        messages.remove(messageId);
      }
    }
    consumer.close();
    assertEquals(messages.size(), 0, "All the messages sent should have been consumed.");
  }


  @Test
  public void testLargeMessageWithRecordHeaderEnabled() throws Exception {
    // this test will set LiKafkaProducerConfig.ENABLE_RECORD_HEADER_CONFIG to be true
    // which means we will test record header based LM support without using segmentSerde

    //create the test topic
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1))).all().get(1, TimeUnit.MINUTES);
    }

    long startTime = System.currentTimeMillis();
    Properties props = new Properties();
    props.setProperty("large.message.enabled", "true");
    props.setProperty("max.message.segment.size", "200");
    props.setProperty("client.id", "testProducer");
    props.setProperty(LiKafkaProducerConfig.ENABLE_RECORD_HEADER_CONFIG, "true");

    LiKafkaProducer<String, String> largeMessageProducer = createProducer(props);
    Properties consumerProps = buildConsumerProps();
    consumerProps.setProperty("auto.offset.reset", "earliest");
    LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps);

        /* The test will send 100 different large messages to broker, consume from broker and verify the message contents.
           Here for simplicity we use a large message segment as a large message, and chunk this */
    Map<String, String> messages = new HashMap<>();
    int numberOfLargeMessages = 100;
    int largeMessageSize = 1000;
    final Set<String> ackedMessages = new HashSet<>();
    // Produce large messages.
    for (int i = 0; i < numberOfLargeMessages; i++) {
      final String messageId = LiKafkaClientsUtils.randomUUID().toString().replace("-", "");
      String message = messageId + KafkaTestUtils.getRandomString(largeMessageSize);
      messages.put(messageId, message);
      largeMessageProducer.send(new ProducerRecord<>(TOPIC, message), (recordMetadata, e) -> {
        // The callback should have been invoked only once.
        assertFalse(ackedMessages.contains(messageId));
        if (e == null) {
          ackedMessages.add(messageId);
        }
      });
    }
    largeMessageProducer.close();
    // All messages should have been sent.
    assertEquals(ackedMessages.size(), messages.size());

    // Consume and verify the large messages
    List<TopicPartition> partitions = new ArrayList<>();
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      partitions.add(new TopicPartition(TOPIC, i));
    }
    consumer.assign(partitions);

    ConsumerRecords<String, String> records = null;
    boolean consumptionStarted = false;
    while (!consumptionStarted || records.count() > 0) {
      records = consumer.poll(10);
      if (records.count() > 0) {
        consumptionStarted = true;
      }
      for (ConsumerRecord<String, String> consumerRecord : records) {
        // Verify headers
        Long eventTimestamp = LiKafkaClientsUtils.fetchTimestampHeader(consumerRecord.headers());
        assertNotNull(eventTimestamp);
        assertTrue(eventTimestamp >= startTime && eventTimestamp <= System.currentTimeMillis());
        LargeMessageHeaderValue largeMessageHeaderValue = LiKafkaClientsUtils.fetchLargeMessageHeader(consumerRecord.headers());
        assertEquals(largeMessageHeaderValue.getSegmentNumber(), -1);
        assertEquals(largeMessageHeaderValue.getNumberOfSegments(), 6);
        // V3 means we use record header based LM         
        assertEquals(largeMessageHeaderValue.getType(), LargeMessageHeaderValue.V3);

        String messageId = consumerRecord.value().substring(0, 32);
        String origMessage = messages.get(messageId);
        assertEquals(consumerRecord.value(), origMessage, "Messages should be the same");
        messages.remove(messageId);
      }
    }
    consumer.close();
    assertEquals(messages.size(), 0, "All the messages sent should have been consumed.");
  }

  private static Properties buildConsumerProps() {
    Properties props = new Properties();
    props.setProperty(CLIENT_ID_CONFIG, "testLargeMessageConsumer");
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testLargeMessageConsumer");
    props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
    props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "blah");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
    props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");
    props.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "30000");
    props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576");
    props.setProperty(ConsumerConfig.SEND_BUFFER_CONFIG, "131072");
    props.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "32768");
    props.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "50");
    props.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "6000");
    props.setProperty(ConsumerConfig.CHECK_CRCS_CONFIG, "true");
    props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor");
    return props;
  }
}
