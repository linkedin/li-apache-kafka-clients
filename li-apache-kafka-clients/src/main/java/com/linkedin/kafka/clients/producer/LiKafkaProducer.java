/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.annotations.InterfaceOrigin;
import java.util.Set;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;


/**
 * The general producer interface that allows allows pluggable serializers and deserializers.
 * LiKafkaProducer has the same interface as open source {@link Producer}. We define the interface separately to allow
 * future extensions.
 * @see LiKafkaProducerImpl
 */
public interface LiKafkaProducer<K, V> extends Producer<K, V> {

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  Future<RecordMetadata> send(ProducerRecord<K, V> record);

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  void flush();

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  List<PartitionInfo> partitionsFor(String topic);

  Map<String, List<PartitionInfo>> partitionsFor(Set<String> topics);

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  Map<MetricName, ? extends Metric> metrics();

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  void close();

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  void close(long timeout, TimeUnit unit);

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  void initTransactions();

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  void beginTransaction() throws ProducerFencedException;

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException;

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  void commitTransaction() throws ProducerFencedException;

  /**
   * {@inheritDoc}
   */
  @InterfaceOrigin.ApacheKafka
  void abortTransaction() throws ProducerFencedException;

}
