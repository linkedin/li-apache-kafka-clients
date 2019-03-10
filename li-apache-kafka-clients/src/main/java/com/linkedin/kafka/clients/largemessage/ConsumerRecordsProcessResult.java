/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.largemessage.errors.ConsumerRecordsProcessingException;
import com.linkedin.kafka.clients.largemessage.errors.RecordProcessingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;


/**
 * The process result of ConsumerRecords returned by the open source KafkaConsumer.
 *
 * It contains the following information:
 * 1. The processed consumer records.
 * 2. If there were exception in processing, the offsets to skip those problematic messages for each partition.
 * 3. The the exception thrown by the last problematic partition. (We just need to throw an exception to the user).
 */
public class ConsumerRecordsProcessResult<K, V> {
  public class OffsetPair {
    private final long _currentOffset;
    private final long _resumeOffset;

    public OffsetPair(long currentOffset, long resumeOffset) {
      _currentOffset = currentOffset;
      _resumeOffset = resumeOffset;
    }

    public long getCurrentOffset() {
      return _currentOffset;
    }

    public long getResumeOffset() {
      return _resumeOffset;
    }
  }

  private final Map<TopicPartition, OffsetPair> _offsetPair;
  private final List<RecordProcessingException> _exceptions;
  private final Map<TopicPartition, RecordProcessingException> _exceptionMap;
  private Map<TopicPartition, List<ConsumerRecord<K, V>>> _processedRecords;

  ConsumerRecordsProcessResult() {
    _processedRecords = new HashMap<>();
    _offsetPair = new HashMap<>();
    _exceptions = new ArrayList<>();
    _exceptionMap = new HashMap<>();
  }

  void addRecord(TopicPartition tp, ConsumerRecord<K, V> record) {
    // Only put record into map if it is not null
    if (record != null) {
      List<ConsumerRecord<K, V>> list = _processedRecords.computeIfAbsent(tp, k -> new ArrayList<>());
      list.add(record);
    }
  }

  void recordException(TopicPartition tp, long offset, RuntimeException e) {
    RecordProcessingException rpe = new RecordProcessingException(tp, offset, e);
    _exceptions.add(rpe);
    _exceptionMap.put(tp, rpe);
    // The resume offset is the error offset + 1. i.e. if user ignore the exception thrown and poll again, the resuming
    // offset should be this one.
    _offsetPair.putIfAbsent(tp, new OffsetPair(offset, offset + 1));
  }

  public void clearRecords() {
    _processedRecords = null;
  }

  public boolean hasError(TopicPartition tp) {
    return offsets().containsKey(tp);
  }

  /**
   * Returns true if any topic-partitions in the collection has an exception
   */
  public boolean hasError(Collection<TopicPartition> topicPartitions) {
    if (topicPartitions != null && !topicPartitions.isEmpty()) {
      for (TopicPartition tp : topicPartitions) {
        if (offsets().containsKey(tp)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean hasException() {
    return !_exceptionMap.isEmpty();
  }

  public ConsumerRecordsProcessingException exception() {
    return _exceptions.isEmpty() ? null : new ConsumerRecordsProcessingException(_exceptions);
  }

  public ConsumerRecordsProcessingException exception(Collection<TopicPartition> topicPartitions) {
    List<RecordProcessingException> recordProcessingExceptions = new ArrayList<>(topicPartitions.size());
    topicPartitions.forEach(tp -> {
      if (_exceptionMap.containsKey(tp)) {
        recordProcessingExceptions.add(_exceptionMap.get(tp));
      }
    });

    return _exceptions.isEmpty() ? null : new ConsumerRecordsProcessingException(recordProcessingExceptions);
  }

  public ConsumerRecords<K, V> consumerRecords() {
    return new ConsumerRecords<>(_processedRecords);
  }

  public Map<TopicPartition, OffsetPair> offsets() {
    return Collections.unmodifiableMap(_offsetPair);
  }
}

