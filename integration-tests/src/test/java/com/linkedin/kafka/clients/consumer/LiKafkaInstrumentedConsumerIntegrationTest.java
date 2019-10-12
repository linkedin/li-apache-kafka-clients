/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.google.common.collect.ImmutableMap;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import com.linkedin.kafka.clients.utils.tests.KafkaTestUtils;
import com.linkedin.mario.common.models.v1.ClientConfigRule;
import com.linkedin.mario.common.models.v1.ClientConfigRules;
import com.linkedin.mario.common.models.v1.ClientPredicates;
import com.linkedin.mario.server.MarioApplication;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LiKafkaInstrumentedConsumerIntegrationTest extends AbstractKafkaClientsIntegrationTestHarness {

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
  public void testConsumerLiveConfigReload() throws Exception {
    String topic = "testConsumerLiveConfigReload";
    createTopic(topic, 1);
    Producer<byte[], byte[]> producer = createRawProducer();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[1024];
      byte[] value = new byte[1024];
      Arrays.fill(key, (byte) i);
      Arrays.fill(value, (byte) i);
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, 0, key, value);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      dos.writeInt(i);
      dos.close();
      record.headers().add("recordNum", bos.toByteArray());
      producer.send(record);
    }
    producer.flush();
    producer.close(1, TimeUnit.MINUTES);

    MarioApplication mario = new MarioApplication(null);
    Random random = new Random();
    int beforeBatchSize = 1 + random.nextInt(20); //[1, 20]

    Properties extra = new Properties();
    extra.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    extra.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    extra.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "" + beforeBatchSize);
    Properties baseConsumerConfig = getConsumerProperties(extra);
    LiKafkaInstrumentedConsumerImpl<byte[], byte[]> consumer = new LiKafkaInstrumentedConsumerImpl<>(
        baseConsumerConfig,
        null,
        (baseConfig, overrideConfig) -> new LiKafkaConsumerImpl<>(LiKafkaClientsUtils.getConsolidatedProperties(baseConfig, overrideConfig)),
        mario::getUrl);

    consumer.subscribe(Collections.singletonList(topic));
    AtomicReference<ConsumerRecords<byte[], byte[]>> recordsRef = new AtomicReference<>(null);
    AtomicReference<Consumer<byte[], byte[]>> delegateBeforeRef = new AtomicReference<>(null);
    KafkaTestUtils.waitUntil("1st record batch", () -> {
      ConsumerRecords<byte[], byte[]> recs = consumer.poll(Duration.ofSeconds(10));
      if (recs.count() > 0) {
        recordsRef.set(recs);
        delegateBeforeRef.set(consumer.getDelegate());
        return true;
      }
      return false;
    }, 1, 2, TimeUnit.MINUTES, false);
    ConsumerRecords<byte[], byte[]> firstBatch = recordsRef.get(); //guaranteed != null
    Consumer<byte[], byte[]> delegate = delegateBeforeRef.get();

    Assert.assertEquals(firstBatch.count(), beforeBatchSize);

    TopicPartition p0 = new TopicPartition(topic, 0);
    List<ConsumerRecord<byte[], byte[]>> records = firstBatch.records(p0);
    ConsumerRecord<byte[], byte[]> lastRecordInFirstBatch = records.get(records.size() - 1);

    int afterBatchSize = 31 + random.nextInt(20); //[31, 50]

    //install a new config policy, wait for the push
    mario.setConfigPolicy(new ClientConfigRules(Collections.singletonList(
        new ClientConfigRule(ClientPredicates.ALL, ImmutableMap.of("max.poll.records", "" + afterBatchSize)))));

    KafkaTestUtils.waitUntil("delegate recreated", () -> {
      Consumer<byte[], byte[]> delegateNow = consumer.getDelegate();
      return delegateNow != delegate;
    }, 1, 2, TimeUnit.MINUTES, false);

    KafkaTestUtils.waitUntil("1nd record batch", () -> {
      ConsumerRecords<byte[], byte[]> recs = consumer.poll(Duration.ofSeconds(10));
      if (recs.count() > 0) {
        recordsRef.set(recs);
        return true;
      }
      return false;
    }, 1, 2, TimeUnit.MINUTES, false);

    ConsumerRecords<byte[], byte[]> secondBatch = recordsRef.get(); //guaranteed != null
    Assert.assertEquals(secondBatch.count(), afterBatchSize);

    records = secondBatch.records(p0);
    ConsumerRecord<byte[], byte[]> firstRecordInSecondBatch = records.get(0);

    //make sure no skips
    Assert.assertEquals(firstRecordInSecondBatch.offset(), lastRecordInFirstBatch.offset() + 1,
        lastRecordInFirstBatch.offset() + " + 1 != " + firstRecordInSecondBatch.offset());

    consumer.close(Duration.ofSeconds(30));
    mario.close();
  }

  private void createTopic(String topicName, int numPartitions) throws Exception {
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, numPartitions, (short) 1))).all().get(1, TimeUnit.MINUTES);
    }
  }
}
