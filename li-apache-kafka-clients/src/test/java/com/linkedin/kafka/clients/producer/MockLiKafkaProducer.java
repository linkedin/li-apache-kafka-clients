/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;


// Mock LiKafkaProducer with raw byte key and value
public class MockLiKafkaProducer implements LiKafkaProducer<byte[], byte[]> {
  private MockProducer<byte[], byte[]> _delegate;

  public MockLiKafkaProducer() {
    _delegate = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> producerRecord) {
    return send(producerRecord, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> producerRecord, Callback callback) {
    return _delegate.send(producerRecord, callback);
  }

  @Override
  public void flush() {
    flush(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  public void flush(long timeout, TimeUnit timeUnit) {
    // Timeout is not really tested here.
    _delegate.flush();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return _delegate.partitionsFor(topic);
  }

  @Override
  public Map<String, List<PartitionInfo>> partitionsFor(Set<String> topics) {
    //TODO come back here when upstream API settles
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close() {
    close(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
    _delegate.close(timeout, timeUnit);
  }

  @Override
  public void initTransactions() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
      String consumerGroupId) throws ProducerFencedException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException("Not supported");
  }

  public MockProducer<byte[], byte[]> getDelegate() {
    return _delegate;
  }
}
