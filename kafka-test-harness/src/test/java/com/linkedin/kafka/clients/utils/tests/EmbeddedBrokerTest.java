/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.util.Collections;
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
        KafkaProducer<String, String> producer = KafkaTestUtils.vanillaProducerFor(broker);
        KafkaConsumer<String, String> consumer = KafkaTestUtils.vanillaConsumerFor(broker)) {

      RecordMetadata md = producer.send(new ProducerRecord<>("topic", "key", "value")).get();
      Assert.assertNotNull(md);
      Assert.assertEquals(0, md.offset()); //1st msg

      consumer.subscribe(Collections.singletonList("topic"));
      ConsumerRecords<String, String> records = consumer.poll(10000);

      Assert.assertEquals(1, records.count());
    }
  }
}
