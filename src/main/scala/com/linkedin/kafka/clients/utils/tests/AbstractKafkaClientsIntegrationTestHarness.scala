/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests

import java.util.Properties

import com.linkedin.kafka.clients.consumer.{LiKafkaConsumerConfig, LiKafkaConsumer, LiKafkaConsumerImpl}
import com.linkedin.kafka.clients.largemessage.{DefaultSegmentSerializer, DefaultSegmentDeserializer}
import com.linkedin.kafka.clients.producer.{LiKafkaProducerConfig, LiKafkaProducer, LiKafkaProducerImpl}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

/**
 * Integration test harness for likafka-clients
 */
abstract class AbstractKafkaClientsIntegrationTestHarness extends AbstractKafkaIntegrationTestHarness {

  /**
   * Generate a new LiKafkaProducer which has all the settings configured to produce data.
   * @param props The properties to be used to override default producer configurations.
   * @return A LiKafkaProducer which is ready to use.
   */
  def createProducer(props: Properties = null): LiKafkaProducer[String, String] = {
    val producerProps = Option(props).getOrElse(new Properties())
    maybeSetProperties(producerProps, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapUrl)
    maybeSetProperties(producerProps, LiKafkaProducerConfig.MAX_MESSAGE_SEGMENT_BYTES_CONFIG, "200")
    maybeSetProperties(producerProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    maybeSetProperties(producerProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    maybeSetProperties(producerProps, LiKafkaProducerConfig.SEGMENT_SERIALIZER_CLASS_CONFIG, classOf[DefaultSegmentSerializer].getName)

    val stringSerializer = new StringSerializer
    new LiKafkaProducerImpl[String, String](producerProps, stringSerializer, stringSerializer, null, null)
  }

  /**
   * Generate a new LiKafkaConsumer which has all the settings configured to consume data.
   * @param props The properties to be used to override default consumer configurations.
   * @return A LiKafkaConsumer which is ready to use.
   */
  def createConsumer(props: Properties = null): LiKafkaConsumer[String, String] = {
    new LiKafkaConsumerImpl(getConsumerProperties(props))
  }

  /**
   * Get the default consumer properties.
   * @param props the properties that users want to specify
   * @return the consumer properties ready to instantiate a consumer
   */
  def getConsumerProperties(props: Properties = null): Properties = {
    val consumerProps = Option(props).getOrElse(new Properties())
    maybeSetProperties(consumerProps, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapUrl)
    maybeSetProperties(consumerProps, ConsumerConfig.GROUP_ID_CONFIG, "testingConsumer")
    maybeSetProperties(consumerProps, LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_BUFFER_CAPACITY_CONFIG, "300000")
    maybeSetProperties(consumerProps, LiKafkaConsumerConfig.MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_CONFIG, "10000")
    maybeSetProperties(consumerProps, LiKafkaConsumerConfig.EXCEPTION_ON_MESSAGE_DROPPED_CONFIG, "true")
    maybeSetProperties(consumerProps, LiKafkaConsumerConfig.MAX_TRACKED_MESSAGES_PER_PARTITION_CONFIG, "10000")
    maybeSetProperties(consumerProps, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    maybeSetProperties(consumerProps, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    maybeSetProperties(consumerProps, LiKafkaConsumerConfig.SEGMENT_DESERIALIZER_CLASS_CONFIG, classOf[DefaultSegmentDeserializer].getName)
    consumerProps
  }

  /**
    * Sets a value if props does not already have the key.
    */
  def maybeSetProperties(props: Properties, key: String, value: String) {
    if (!props.containsKey(key))
      props.setProperty(key, value);
  }

}
