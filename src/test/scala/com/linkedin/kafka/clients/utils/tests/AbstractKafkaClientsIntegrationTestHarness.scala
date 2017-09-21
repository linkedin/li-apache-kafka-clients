/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests

import java.util.Properties

import com.linkedin.kafka.clients.consumer.{LiKafkaConsumer, LiKafkaConsumerConfig, LiKafkaConsumerImpl}
import com.linkedin.kafka.clients.largemessage.{DefaultSegmentDeserializer, DefaultSegmentSerializer}
import com.linkedin.kafka.clients.producer.{LiKafkaProducer, LiKafkaProducerConfig, LiKafkaProducerImpl}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringDeserializer, StringSerializer}

/**
 * Integration test harness for likafka-clients
 */
abstract class AbstractKafkaClientsIntegrationTestHarness extends AbstractKafkaIntegrationTestHarness {

  lazy val usingSSL: Boolean = securityProtocol == SecurityProtocol.SSL

  /**
   * Generate a new LiKafkaProducer which has all the settings configured to produce data.
   * @param props The properties to be used to override default producer configurations.
   * @return A LiKafkaProducer which is ready to use.
   */
  def createProducer(props: Properties = null): LiKafkaProducer[String, String] = {
    new LiKafkaProducerImpl[String, String](getProducerProperties(props))
  }

  def createKafkaProducer(): Producer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    val finalProducerProps = getProducerProperties(props)
    new KafkaProducer(finalProducerProps)
  }

  /**
    * Get the default producer properties.
    * @param props the properties that users want to specify
    * @return the producer properties ready to instantiate a producer
    */
  def getProducerProperties(props: Properties = null): Properties = {
    val producerProps = Option(props).getOrElse(new Properties())
    if (usingSSL) {
      producerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
      producerProps.putAll(TestUtils.sslConfigs(Mode.CLIENT, clientCert = true, trustStoreFile, "producer"))
    }
    maybeSetProperties(producerProps, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapUrl)
    maybeSetProperties(producerProps, LiKafkaProducerConfig.MAX_MESSAGE_SEGMENT_BYTES_CONFIG, "200")
    maybeSetProperties(producerProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    maybeSetProperties(producerProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    maybeSetProperties(producerProps, LiKafkaProducerConfig.SEGMENT_SERIALIZER_CLASS_CONFIG, classOf[DefaultSegmentSerializer].getName)
    producerProps
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
    if (usingSSL) {
      consumerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
      consumerProps.putAll(TestUtils.sslConfigs(Mode.CLIENT, clientCert = true, trustStoreFile, "consumer"))
    }
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
      props.setProperty(key, value)
  }

}
