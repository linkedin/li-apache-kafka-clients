/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.auditing;

import com.linkedin.kafka.clients.producer.LiKafkaProducerConfig;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The auditor interface. This class allows user to implement their own auditing solution.
 *
 * Notice that the auditor may be used by multiple threads, so the implementation should be thread safe.
 */
public interface Auditor<K, V> extends Configurable {

  /**
   * This method will be invoked by LiKafkaProducer on instantiation.
   * Notice that if the auditor is used by the producer, there will be an additional
   * {@link KafkaProducer KafkaProducer&lt;byte[], byte[]&gt;} object passed in the configuration with the key of
   * {@link LiKafkaProducerConfig#CURRENT_PRODUCER}. User can use this producer send auditing events to the same Kafka
   * cluster the producer is is producing to. This is to avoid creating another producer.
   *
   * @param configs The configurations for the auditor
   */
  void configure(Map<String, ?> configs);

  /**
   * Start the auditor.
   */
  void start();

  /**
   * Get the audit token from the key and value of the record.
   * This method helps the producer avoid holding the key and value until the message sending is completed.
   *
   * @param key the key of the record.
   * @param value the value of the record.
   * @return the custom audit information.
   */
  Object auditToken(K key, V value);

  /**
   * Audit the record based on the given information.
   *
   * @param auditToken The user extracted auditing information.
   * @param topic The topic of the record.
   * @param timestamp The timestamp of the record.
   * @param messageCount The number of messages to record.
   * @param bytesCount The number of bytes to record.
   * @param auditType The type of the auditing action.
   */
  void record(Object auditToken,
              String topic,
              Long timestamp,
              Long messageCount,
              Long bytesCount,
              AuditType auditType);

  /**
   * Close the auditor with timeout.
   * This method will be called when producer is closed with a timeout.
   *
   * @param timeout the maximum time to wait to close the auditor.
   * @param unit The time unit.
   */
  void close(long timeout, TimeUnit unit);

  /**
   * The LiKafkaProducer and LiKafkaConsumer will call this method when the producer or consumer is closed.
   * Close the auditor.
   */
  void close();


}
