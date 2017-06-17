/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.auditing.abstractimpl;

import com.linkedin.kafka.clients.auditing.NoOpAuditor;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumer;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumerImpl;
import com.linkedin.kafka.clients.producer.LiKafkaProducer;
import com.linkedin.kafka.clients.producer.LiKafkaProducerConfig;
import com.linkedin.kafka.clients.producer.LiKafkaProducerImpl;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;

public class ConfigureAuditorTest extends AbstractKafkaClientsIntegrationTestHarness {
  @BeforeMethod
  @Override
  public void setUp() {
    brokerList_$eq("localhost:9092");
  }

  @Test
  public void testProducerConfigure() {
    Properties props = new Properties();
    props.setProperty(LiKafkaProducerConfig.AUDITOR_CLASS_CONFIG, TestAuditor.class.getName());

    LiKafkaProducer<String, String> producerConfig = createProducer(props);
    assertEquals(1, TestAuditor.configureMethodInvocations.get().intValue());
    producerConfig.close();

    final TestAuditor producerAuditor = new TestAuditor();
    producerAuditor.configure(props);
    LiKafkaProducer producerInstance = new LiKafkaProducerImpl(getProducerProperties(new Properties()),
        null, null, null, producerAuditor);
    assertEquals(1, TestAuditor.configureMethodInvocations.get().intValue());
    producerInstance.close();
  }

  @Test
  public void testConsumerConfigure() {
    Properties props = new Properties();
    props.setProperty(LiKafkaProducerConfig.AUDITOR_CLASS_CONFIG, TestAuditor.class.getName());

    LiKafkaConsumer<String, String> producerConfig = createConsumer(props);
    assertEquals(1, TestAuditor.configureMethodInvocations.get().intValue());
    producerConfig.close();

    final TestAuditor consumerAuditor = new TestAuditor();
    consumerAuditor.configure(props);
    LiKafkaConsumerImpl producerInstance = new LiKafkaConsumerImpl(getConsumerProperties(new Properties()),
        null, null, null, consumerAuditor);
    assertEquals(1, TestAuditor.configureMethodInvocations.get().intValue());
    producerInstance.close();
  }

  public static class TestAuditor extends NoOpAuditor {
    private static ThreadLocal<Integer> configureMethodInvocations = ThreadLocal.withInitial(() -> 0);

    @Override
    public void configure(Map configs) {
      configureMethodInvocations.set(configureMethodInvocations.get() + 1);
    }

    @Override
    public void close() {
      configureMethodInvocations.set(0);
    }
  }
}
