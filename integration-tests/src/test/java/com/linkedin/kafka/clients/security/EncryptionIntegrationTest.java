/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.security;

import com.linkedin.kafka.clients.common.LargeMessageHeaderValue;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumer;
import com.linkedin.kafka.clients.producer.LiKafkaProducer;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import com.linkedin.kafka.clients.utils.tests.KafkaTestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.kafka.clients.producer.LiKafkaProducerConfig.*;
import static com.linkedin.kafka.clients.utils.LiKafkaClientsTestUtils.*;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;




/**
 * The integration test for encryption.
 */
public class EncryptionIntegrationTest extends AbstractKafkaClientsIntegrationTestHarness {
  private static final String TOPIC = "TestEncryption";
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
  public void testEncryptionWithNormalRecords() throws Exception {
    //create the test topic
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1)))
          .all()
          .get(1, TimeUnit.MINUTES);
    }

    {
      long startTime = System.currentTimeMillis();
      Properties props = buildProducerProps(true, false);
      LiKafkaProducer<String, String> producer = createProducer(props);

      Properties consumerProps = buildConsumerProps();
      consumerProps.setProperty("auto.offset.reset", "earliest");
      LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps);


      /* The test will send 100 different encrypted messages(not large) to broker, consume from broker and verify the message contents. */
      Map<String, String> messages = new HashMap<>();
      int numberOfMessages = 100;
      int messageSize = 10;
      final Set<String> ackedMessages = new HashSet<>();
      // Produce messages.
      for (int i = 0; i < numberOfMessages; i++) {
        final String messageId = LiKafkaClientsUtils.randomUUID().toString().replace("-", "");
        String message = messageId + KafkaTestUtils.getRandomString(messageSize);
        messages.put(messageId, message);

        // This is expected size of final message after encryption
        final int expectedProducedMessageSize = getEncryptedMessageValueSize(message);

        producer.send(new ProducerRecord<>(TOPIC, message), (recordMetadata, e) -> {
          assertEquals(recordMetadata.serializedValueSize(), expectedProducedMessageSize);
          // The callback should have been invoked only once.
          assertFalse(ackedMessages.contains(messageId));
          if (e == null) {
            ackedMessages.add(messageId);
          }
        });
      }
      producer.close();
       // All messages should have been sent.
      assertEquals(ackedMessages.size(), messages.size());

      // Consume and verify the encrypted messages
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

          Integer encryptedFlag = LiKafkaClientsUtils.fetchEncryptionHeader(consumerRecord.headers());
          assertNotNull(encryptedFlag);

          assertTrue(eventTimestamp >= startTime && eventTimestamp <= System.currentTimeMillis());
          assertEquals(1, encryptedFlag.intValue());
          String messageId = consumerRecord.value().substring(0, 32);
          String origMessage = messages.get(messageId);
          assertEquals(consumerRecord.value(), origMessage, "Messages should be the same");
          messages.remove(messageId);
        }
      }
      consumer.close();
      assertEquals(messages.size(), 0, "All the messages sent should have been consumed.");
    }
  }

  @Test
  public void testEncryptionWitLargeMessage() throws Exception {
    //create the test topic
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1))).all().get(1, TimeUnit.MINUTES);
    }

    long startTime = System.currentTimeMillis();
    Properties props = buildProducerProps(true, true);
    LiKafkaProducer<String, String> largeMessageProducer = createProducer(props);
    Properties consumerProps = buildConsumerProps();
    consumerProps.setProperty("auto.offset.reset", "earliest");
    LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps);

        /* The test will send 100 different encrypted large messages to broker, consume from broker and verify the message contents.
           For simplicity we use a large message segment as a large message, and chunk this */
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

        Integer encryptedFlag = LiKafkaClientsUtils.fetchEncryptionHeader(consumerRecord.headers());
        assertNotNull(encryptedFlag);
        assertEquals(1, encryptedFlag.intValue());
        LargeMessageHeaderValue largeMessageHeaderValue = LiKafkaClientsUtils.fetchLargeMessageHeader(consumerRecord.headers());
        assertNotNull(largeMessageHeaderValue);
        assertEquals(largeMessageHeaderValue.getSegmentNumber(), -1);
        assertEquals(largeMessageHeaderValue.getNumberOfSegments(), 7);
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
  public void testConsumeEncryptedAndNormalRecords() throws Exception {
    //create the test topic
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1)))
          .all()
          .get(1, TimeUnit.MINUTES);
    }

    long startTime = System.currentTimeMillis();
    Properties props = buildProducerProps(true, true);
    LiKafkaProducer<String, String> largeMessageProducer = createProducer(props);
    Properties props2 = buildProducerProps(false, true);
    LiKafkaProducer<String, String> largeMessageProducer2 = createProducer(props2);


    Properties consumerProps = buildConsumerProps();
    consumerProps.setProperty("auto.offset.reset", "earliest");
    LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps);

        /* The test will send 100 different encrypted large messages to broker, consume from broker and verify the message contents.
           For simplicity we use a large message segment as a large message, and chunk this */
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

        Integer encryptedFlag = LiKafkaClientsUtils.fetchEncryptionHeader(consumerRecord.headers());
        assertNotNull(encryptedFlag);
        assertEquals(1, encryptedFlag.intValue());
        LargeMessageHeaderValue largeMessageHeaderValue = LiKafkaClientsUtils.fetchLargeMessageHeader(consumerRecord.headers());
        assertEquals(largeMessageHeaderValue.getSegmentNumber(), -1);
        assertEquals(largeMessageHeaderValue.getNumberOfSegments(), 7);
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
  private static Properties buildProducerProps(boolean isEncrypted, boolean isLargeMessageEnabled) {
    Properties props = new Properties();
    props.setProperty(MAX_MESSAGE_SEGMENT_BYTES_CONFIG, "200");
    props.setProperty(CLIENT_ID_CONFIG, "testProducer");
    props.setProperty(ENCRYPTION_ENABLED_CONFIG, String.valueOf(isEncrypted));
    props.setProperty(LARGE_MESSAGE_ENABLED_CONFIG, String.valueOf(isLargeMessageEnabled));
    props.setProperty(LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_CONFIG, String.valueOf(isLargeMessageEnabled));
    return props;
  }

  private static int getEncryptedMessageValueSize(String text) {
    return 4 * ((int) Math.ceil(text.length() / 3.0));
  }
}


