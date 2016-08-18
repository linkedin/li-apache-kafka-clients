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

import com.linkedin.kafka.clients.annotations.InterfaceOrigin;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The general producer interface that allows allows pluggable serializers and deserializers.
 */
public interface LiKafkaProducer<K, V> extends Producer<K, V> {
  /**
   * Send the given record asynchronously and return a future which will eventually contain the response information.
   *
   * @param record The record to send
   * @return A future which will eventually contain the response information
   */
  @InterfaceOrigin.ApacheKafka
  Future<RecordMetadata> send(ProducerRecord<K, V> record);

  /**
   * Send a record and invoke the given callback when the record has been acknowledged by the server
   */
  @InterfaceOrigin.ApacheKafka
  Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

  /**
   * Flush any accumulated records from the producer. Blocks until all sends are complete.
   */
  @InterfaceOrigin.ApacheKafka
  void flush();

  /**
   * Get a list of partitions for the given topic for custom partition assignment. The partition metadata will change
   * over time so this list should not be cached.
   */
  @InterfaceOrigin.ApacheKafka
  List<PartitionInfo> partitionsFor(String topic);

  /**
   * Return a map of metrics maintained by the producer
   */
  @InterfaceOrigin.ApacheKafka
  Map<MetricName, ? extends Metric> metrics();

  /**
   * LiKafkaProducer method.
   * Get the UUID that will be used for the segments of the original large message.
   *
   * @param key   key of the original large message.
   * @param value value of the original large message.
   * @return UUID to use for the large message segments.
   */
  @InterfaceOrigin.LiKafkaClients
  UUID getUuid(K key, V value);

  /**
   * Close this producer
   */
  @InterfaceOrigin.ApacheKafka
  void close();

  /**
   * Tries to close the producer cleanly within the specified timeout. If the close does not complete within the
   * timeout, fail any pending send requests and force close the producer.
   */
  @InterfaceOrigin.ApacheKafka
  void close(long timeout, TimeUnit unit);
}
