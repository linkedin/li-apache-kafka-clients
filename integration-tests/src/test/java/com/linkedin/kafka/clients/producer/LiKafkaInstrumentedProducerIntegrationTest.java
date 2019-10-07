/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import com.linkedin.mario.common.models.v1.ClientConfigRule;
import com.linkedin.mario.common.models.v1.ClientConfigRules;
import com.linkedin.mario.common.models.v1.ClientPredicates;
import com.linkedin.mario.server.MarioApplication;
import com.linkedin.mario.test.TestUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LiKafkaInstrumentedProducerIntegrationTest extends AbstractKafkaClientsIntegrationTestHarness {

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
  public void testProducerLiveConfigReload() throws Exception {
    String topic = "testProducerLiveConfigReload";
    createTopic(topic, 1);
    MarioApplication mario = new MarioApplication(null);
    Random random = new Random();

    Properties extra = new Properties();
    extra.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "" + 1500);
    extra.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    extra.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    extra.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    Properties baseProducerConfig = getProducerProperties(extra);
    LiKafkaInstrumentedProducerImpl<byte[], byte[]> producer = new LiKafkaInstrumentedProducerImpl<>(
        baseProducerConfig,
        Collections.emptyMap(),
        (baseConfig, overrideConfig) -> new LiKafkaProducerImpl<>(LiKafkaClientsUtils.getConsolidatedProperties(baseConfig, overrideConfig)),
        mario::getUrl);

    byte[] key = new byte[500];
    byte[] value = new byte[500];
    random.nextBytes(key);
    random.nextBytes(value);
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, 0, key, value);
    RecordMetadata recordMetadata = producer.send(record).get();

    Producer<byte[], byte[]> delegate = producer.getDelegate();

    key = new byte[3000];
    value = new byte[3000];
    random.nextBytes(key);
    random.nextBytes(value);
    record = new ProducerRecord<>(topic, 0, key, value);
    try {
      producer.send(record).get();
      Assert.fail("record expected to fail");
    } catch (Exception e) {
      Throwable root = Throwables.getRootCause(e);
      Assert.assertTrue(root instanceof RecordTooLargeException, root.getClass() + " is not a RecordTooLargeException");
    }

    //install a new config policy, wait for the push
    mario.setConfigPolicy(new ClientConfigRules(Collections.singletonList(
        new ClientConfigRule(ClientPredicates.ALL, ImmutableMap.of("max.request.size", "" + 9000)))));

    TestUtils.waitUntil("delegate recreated", () -> {
      Producer<byte[], byte[]> delegateNow = producer.getDelegate();
      return delegateNow != delegate;
    }, 1, 2, TimeUnit.MINUTES, false);

    producer.send(record).get(); //should succeed this time

    producer.close(Duration.ofSeconds(30));
    mario.close();
  }

  private void createTopic(String topicName, int numPartitions) throws Exception {
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, numPartitions, (short) 1))).all().get(1, TimeUnit.MINUTES);
    }
  }
}
