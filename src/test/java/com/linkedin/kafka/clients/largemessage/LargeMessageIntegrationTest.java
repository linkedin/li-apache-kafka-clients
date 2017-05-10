/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.consumer.LiKafkaConsumer;
import com.linkedin.kafka.clients.producer.LiKafkaProducer;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.TestUtils;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * The integration test for large message.
 * This class has to be in scala source directory because it depends on the scala classes which will only be
 * compiled after java classes are compiled.
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
    props.setProperty(KafkaConfig.NumPartitionsProp(), Integer.toString(NUM_PARTITIONS));
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
  public void testLargeMessage() {
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
    Map<String, String> messages = new HashMap<String, String>();
    int numberOfLargeMessages = 100;
    int largeMessageSize = 1000;
    final Set<String> ackedMessages = new HashSet<String>();
    // Produce large messages.
    for (int i = 0; i < numberOfLargeMessages; i++) {
      final String messageId = LiKafkaClientsUtils.randomUUID().toString().replace("-", "");
      String message = messageId + TestUtils.getRandomString(largeMessageSize);
      messages.put(messageId, message);
      largeMessageProducer.send(new ProducerRecord<String, String>(TOPIC, message),
          new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              // The callback should have been invoked only once.
              assertFalse(ackedMessages.contains(messageId));
              if (e == null) {
                ackedMessages.add(messageId);
              }
            }
          });
    }
    largeMessageProducer.close();
    // All messages should have been sent.
    assertEquals(ackedMessages.size(), messages.size());

    // Consume and verify the large messages
    List<TopicPartition> partitions = new ArrayList<TopicPartition>();
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
    props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testLargeMessageConsumer");
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
