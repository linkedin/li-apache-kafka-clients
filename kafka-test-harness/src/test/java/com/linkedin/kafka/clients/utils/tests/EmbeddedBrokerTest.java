/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;


public class EmbeddedBrokerTest {

  @Test
  public void testSimpleScenario() throws Exception {
    try (EmbeddedZookeeper zk = new EmbeddedZookeeper();
        EmbeddedBroker broker = EmbeddedBroker.newServer().zkConnect(zk).enablePlaintext().build();
        AdminClient adminClient = KafkaTestUtils.adminClientFor(broker);
        KafkaProducer<String, String> producer = KafkaTestUtils.vanillaProducerFor(broker);
        KafkaConsumer<String, String> consumer = KafkaTestUtils.vanillaConsumerFor(broker)) {

      String topicName = "topic";
      adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1))).all().get(1, TimeUnit.MINUTES);
      RecordMetadata md = producer.send(new ProducerRecord<>(topicName, "key", "value")).get();
      Assert.assertNotNull(md);
      Assert.assertEquals(0, md.offset()); //1st msg

      consumer.subscribe(Collections.singletonList(topicName));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

      Assert.assertEquals(1, records.count());
    }
  }
}
