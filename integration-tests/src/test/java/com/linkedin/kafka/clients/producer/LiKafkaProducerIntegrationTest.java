/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.consumer.LiKafkaConsumer;
import com.linkedin.kafka.clients.largemessage.errors.SkippableException;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
   * verifies all data are sent and can be consumed corerctly.
   *
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  @Test
  public void testSend() throws IOException, InterruptedException {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    final String tempTopic = "testTopic" + new Random().nextInt(1000000);
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
  public void testNullValue() {
    try (LiKafkaProducer<String, String> producer = createProducer(null)) {
      producer.send(new ProducerRecord<>("testNullValue", "key", null));
    }
    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (LiKafkaConsumer<String, String> consumer = createConsumer(consumerProps)) {
      consumer.subscribe(Collections.singleton("testNullValue"));
      long startMs = System.currentTimeMillis();
      ConsumerRecords<String, String> records = ConsumerRecords.empty();
      while (records.isEmpty() && System.currentTimeMillis() < startMs + 30000) {
        records = consumer.poll(100);
      }
      assertEquals(1, records.count());
      ConsumerRecord<String, String> record = records.iterator().next();
      assertEquals("key", record.key());
      assertNull(record.value());
    }
  }

  @Test
  public void testZeroLengthValue() throws Exception {
    Properties producerPropertyOverrides = new Properties();
    producerPropertyOverrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    try (LiKafkaProducer producer =  createProducer(producerPropertyOverrides)) {
      producer.send(new ProducerRecord<>("testZeroLengthValue", "key", new byte[0])).get();
    }
    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    try (LiKafkaConsumer consumer = createConsumer(consumerProps)) {
      consumer.subscribe(Collections.singleton("testZeroLengthValue"));
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
  public void testSerializationException() {

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
    final String tempTopic = "testTopic" + new Random().nextInt(1000000);
    try (LiKafkaProducer<String, String> producer =
        new LiKafkaProducerImpl<>(props, stringSerializer, errorThrowingSerializer, null, null)) {
      producer.send(new ProducerRecord<>(tempTopic, "ErrorBytes"));
      producer.send(new ProducerRecord<>(tempTopic, "value"));
      producer.close();
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

}

