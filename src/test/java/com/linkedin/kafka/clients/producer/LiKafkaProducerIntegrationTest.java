/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.consumer.HeaderKeySpace;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumer;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class LiKafkaProducerIntegrationTest extends AbstractKafkaClientsIntegrationTestHarness {
  // Runs testing against running kafka host
  private static final int RECORD_COUNT = 1000;

  @BeforeClass
  @Override
  public void setUp() {
    super.setUp();
  }

  @AfterClass
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

    final byte[] expectedHeaderValue = new byte[] { 1, 2, 3};

    for (int i = 0; i < RECORD_COUNT; ++i) {
      String value = Integer.toString(i);
      ExtensibleProducerRecord producerRecord = new ExtensibleProducerRecord<>(tempTopic, null, null, null, value);
      producerRecord.header(HeaderKeySpace.PUBLIC_UNASSIGNED_START, expectedHeaderValue);
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
        assertEquals(headerValue, expectedHeaderValue);
      }
    }
    consumer.close();
    checkAllMessagesSent(messageCount, counts);
  }

}

