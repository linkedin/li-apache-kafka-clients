/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


// Mock LiKafkaConsumer with raw byte key and value
public class MockLiKafkaConsumer implements LiKafkaConsumer<byte[], byte[]> {
  private MockConsumer<byte[], byte[]> _delegate;

  public MockLiKafkaConsumer(OffsetResetStrategy offsetResetStrategy) {
    _delegate = new MockConsumer<>(offsetResetStrategy);
  }

  @Override
  public Set<TopicPartition> assignment() {
    return _delegate.assignment();
  }

  @Override
  public Set<String> subscription() {
    return _delegate.subscription();
  }

  @Override
  public void subscribe(Collection<String> topics) {
    _delegate.subscribe(topics);
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    _delegate.subscribe(topics, callback);
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    _delegate.assign(partitions);
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    _delegate.subscribe(pattern, callback);
  }

  @Override
  public void subscribe(Pattern pattern) {
    _delegate.subscribe(pattern);
  }

  @Override
  public void unsubscribe() {
    _delegate.unsubscribe();
  }

  @Override
  public ConsumerRecords<byte[], byte[]> poll(long timeout) {
    return _delegate.poll(timeout);
  }

  @Override
  public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
    return _delegate.poll(timeout);
  }

  @Override
  public void commitSync() {
    _delegate.commitSync();
  }

  @Override
  public void commitSync(Duration timeout) {
    _delegate.commitSync(timeout);
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    _delegate.commitSync(offsets);
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    _delegate.commitSync(offsets, timeout);
  }

  @Override
  public void commitAsync() {
    _delegate.commitAsync();
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    _delegate.commitAsync(callback);
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    _delegate.commitAsync(offsets, callback);
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    _delegate.seek(partition, offset);
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    _delegate.seekToBeginning(partitions);
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    _delegate.seekToEnd(partitions);
  }

  @Override
  public void seekToCommitted(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public long position(TopicPartition partition) {
    return _delegate.position(partition);
  }

  @Override
  public long position(TopicPartition partition, Duration timeout) {
    return _delegate.position(partition, timeout);
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    return _delegate.committed(partition);
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
    return _delegate.committed(partition, timeout);
  }

  @Override
  public Long committedSafeOffset(TopicPartition tp) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return _delegate.metrics();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return _delegate.partitionsFor(topic);
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
    return _delegate.partitionsFor(topic, timeout);
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return _delegate.listTopics();
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
    return _delegate.listTopics(timeout);
  }

  @Override
  public Set<TopicPartition> paused() {
    return _delegate.paused();
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    _delegate.pause(partitions);
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    _delegate.resume(partitions);
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
    return _delegate.offsetsForTimes(timestampsToSearch);
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
    return _delegate.offsetsForTimes(timestampsToSearch, timeout);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    return _delegate.beginningOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    return _delegate.beginningOffsets(partitions, timeout);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    return _delegate.endOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    return _delegate.endOffsets(partitions, timeout);
  }

  @Override
  public Long safeOffset(TopicPartition tp, long messageOffset) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Long safeOffset(TopicPartition tp) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<TopicPartition, Long> safeOffsets() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close() {
    _delegate.close();
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
    _delegate.close(timeout, timeUnit);
  }

  @Override
  public void close(Duration timeout) {
    _delegate.close(timeout);
  }

  @Override
  public void wakeup() {
    _delegate.wakeup();
  }

  public MockConsumer<byte[], byte[]> getDelegate() {
    return _delegate;
  }
}
