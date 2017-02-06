/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.consumer.HeaderKeySpace;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumer;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumerConfig;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class LiKafkaProducerIntegrationTest extends AbstractKafkaClientsIntegrationTestHarness {
  // Runs testing against running kafka host
  private static final int RECORD_COUNT = 1000;

  private final static byte[] EXPECTED_HEADER_VALUE = new byte[] { 1, 2, 3};

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
   *
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  @Test
  public void testSend() throws IOException, InterruptedException {

    Properties props = new Properties();
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    LiKafkaProducer<String, String> producer = createProducer(props);
    final String tempTopic = "testTopic" + new Random().nextInt(1000000);

    for (int i = 0; i < RECORD_COUNT; ++i) {
      String value = Integer.toString(i);
      producer.send(new ProducerRecord<>(tempTopic, value));
    }

    // Drain the send queues
    producer.close();

    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps);
    consumer.subscribe(Collections.singleton(tempTopic));
    int messageCount = 0;
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
    consumer.close();
    checkAllMessagesSent(messageCount, counts);
  }

  private void checkAllMessagesSent(int messageCount, BitSet counts) {
    for (int i = 0; i < RECORD_COUNT; i++) {
      assertTrue(counts.get(i));
    }
    assertEquals(RECORD_COUNT, messageCount);
  }

  @Test
  public void testSendHeaderRecord() throws IOException, InterruptedException {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    LiKafkaProducer<String, String> producer = createProducer(props);
    final String tempTopic = "testTopic" + new Random().nextInt(1000000);

    for (int i = 0; i < RECORD_COUNT; ++i) {
      String value = Integer.toString(i);
      ExtensibleProducerRecord producerRecord = new ExtensibleProducerRecord<>(tempTopic, null, null, null, value);
      producerRecord.header(HeaderKeySpace.PUBLIC_UNASSIGNED_START, EXPECTED_HEADER_VALUE);
      producer.send(producerRecord);
    }

    // Drain the send queues
    producer.close();

    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps);
    consumer.subscribe(Collections.singleton(tempTopic));
    int messageCount = 0;
    BitSet counts = new BitSet(RECORD_COUNT);
    long startMs = System.currentTimeMillis();
    while (messageCount < RECORD_COUNT && System.currentTimeMillis() < startMs + 30000) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        ExtensibleConsumerRecord xRecord = (ExtensibleConsumerRecord) record;
        int index = Integer.parseInt(record.value());
        counts.set(index);
        messageCount++;
        byte[] headerValue = xRecord.header(HeaderKeySpace.PUBLIC_UNASSIGNED_START);
        assertEquals(headerValue, EXPECTED_HEADER_VALUE);
      }
    }
    consumer.close();
    checkAllMessagesSent(messageCount, counts);
  }

  /**
   * <pre>
   *   normal size, ProducerRecord
   *   large size, ProducerRecord
   *   normal size, ExtensibleProducerRecord
   *   large size, ExtensibleProducerRecord
   * </pre>
   */
  @Test
  public void produceDifferentRecordScenarios() {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    props.setProperty(LiKafkaProducerConfig.LARGE_MESSAGE_ENABLED_CONFIG, "true");
    props.setProperty(LiKafkaProducerConfig.MAX_MESSAGE_SEGMENT_BYTES_CONFIG, "800000");
    LiKafkaProducer<String, String> producer = createProducer(props);
    String topic = "topic-A";

    int headerKey = HeaderKeySpace.PUBLIC_UNASSIGNED_START + 1;

    String smallValue = com.linkedin.kafka.clients.utils.TestUtils.getRandomString(10);
    String largeValue = com.linkedin.kafka.clients.utils.TestUtils.getRandomString(1024 * 1024);
    ProducerRecord<String, String> largeProducerRecord = new ProducerRecord<>(topic, null, null, null, largeValue);
    ProducerRecord<String, String> smallProducerRecord = new ProducerRecord<>(topic, null, null, null, smallValue);
    ExtensibleProducerRecord<String, String> largeXProducerRecord =
      new ExtensibleProducerRecord<>(topic, null, null, null, largeValue);
    largeXProducerRecord.header(headerKey, EXPECTED_HEADER_VALUE);
    ExtensibleProducerRecord<String, String> smallXProducerRecord =
      new ExtensibleProducerRecord<>(topic, null, null, null, smallValue);
    smallXProducerRecord.header(headerKey, EXPECTED_HEADER_VALUE);

    producer.send(smallProducerRecord);
    producer.send(largeProducerRecord);
    producer.send(smallXProducerRecord);
    producer.send(largeXProducerRecord);
    producer.close();

    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.setProperty(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_BUFFER_CAPACITY_CONFIG, "4000000");
    consumerProps.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000"); // prevent timeout during debugging
    LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps);
    consumer.subscribe(Collections.singleton(topic));

    int messageCount = 0;
    long startMs = System.currentTimeMillis();
    while (messageCount < 4 && System.currentTimeMillis() < startMs + 30000) {

      ConsumerRecords<String, String> consumerRecords = consumer.poll(10);
      for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
        ExtensibleConsumerRecord<String, String> xRecord = (ExtensibleConsumerRecord<String, String>) consumerRecord;
        switch (messageCount) {
          case 0: //small
            assertFalse(xRecord.headerKeys().hasNext());
            assertEquals(xRecord.key(), null);
            assertEquals(xRecord.value(), smallValue);
            break;
          case 1: //large
            assertEquals(xRecord.key(), null);
            assertEquals(xRecord.value(), largeValue);
            break;
          case 2: //small with extra header
            assertEquals(xRecord.key(), null);
            assertEquals(xRecord.value(), smallValue);
            assertEquals(xRecord.header(headerKey), EXPECTED_HEADER_VALUE);
            break;
          case 3: //large with extra header
            assertEquals(xRecord.key(), null);
            assertEquals(xRecord.value(), largeValue);
            assertEquals(xRecord.header(headerKey), EXPECTED_HEADER_VALUE);
            break;
          default:
            throw new IllegalStateException("Unhandled case " + messageCount);
        }
        messageCount++;
      }
    }

    assertEquals(messageCount, 4, "Did not process expected number of messages.");

    consumer.close();
  }

  @Test
  public void nullValue() {
    String key = "somekey";
    String topic = "nullValueTopic";
    byte[] headerValue = {0, -1, -2};
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, null);
    ExtensibleProducerRecord<String, String> xProducerRecord = new ExtensibleProducerRecord<>(topic, null, null, key, null);
    ExtensibleProducerRecord<String, String> xProducerRecordWithHeaders = new ExtensibleProducerRecord<>(topic, null, null, key, null);
    xProducerRecordWithHeaders.header(HeaderKeySpace.PUBLIC_UNASSIGNED_START, headerValue);

    Properties props = new Properties();
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    props.setProperty(LiKafkaProducerConfig.LARGE_MESSAGE_ENABLED_CONFIG, "true");
    props.setProperty(LiKafkaProducerConfig.MAX_MESSAGE_SEGMENT_BYTES_CONFIG, "800000");
    LiKafkaProducer<String, String> producer = createProducer(props);

    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.setProperty(LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_BUFFER_CAPACITY_CONFIG, "4000000");
    consumerProps.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000"); // prevent timeout during debugging
    LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps);
    consumer.subscribe(Collections.singleton(topic));


    producer.send(producerRecord);
    producer.send(xProducerRecord);
    producer.send(xProducerRecordWithHeaders);
    producer.close();

    int messageCount = 0;
    long startMs = System.currentTimeMillis();
    while (messageCount < 3 && System.currentTimeMillis() < startMs + 30000) {
      ConsumerRecords<String, String> consumerRecords = consumer.poll(10);
      for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
        ExtensibleConsumerRecord<String, String> xConsumerRecord = (ExtensibleConsumerRecord<String, String>) consumerRecord;
        switch (messageCount) {
          case 0: // regular producer record with null value
          case 1: // extensible producer record with null value
            assertNull(xConsumerRecord.header(HeaderKeySpace.PUBLIC_UNASSIGNED_START));
            assertEquals(xConsumerRecord.key(), key);
            assertNull(xConsumerRecord.value());
            assertEquals(xConsumerRecord.headersReceivedSizeBytes(), 0);
            break;
          case 2: // extensible producer record with headers
            assertEquals(xConsumerRecord.header(HeaderKeySpace.PUBLIC_UNASSIGNED_START), headerValue);
            assertEquals(xConsumerRecord.key(), key);
            assertNull(xConsumerRecord.value());
            assertTrue(xConsumerRecord.headersReceivedSizeBytes() > 0);
            break;
          default:
            throw new IllegalStateException("Unexpected message.");
        }
        messageCount++;
      }
    }
    assertEquals(messageCount, 3);
    consumer.close();
  }

}


