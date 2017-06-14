package com.linkedin.kafka.clients.largemessage;

import java.util.ArrayList;
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
  private final Map<TopicPartition, List<ConsumerRecord<K, V>>> _processedRecords;
  // This is the offsets of the partitions that should
  private final Map<TopicPartition, Long> _resumeOffsets;
  private RuntimeException _exception;

  ConsumerRecordsProcessResult() {
    _processedRecords = new HashMap<>();
    _resumeOffsets = new HashMap<>();
    _exception = null;
  }

  void addRecord(TopicPartition tp, ConsumerRecord<K, V> record) {
    // Only put record into map if it is not null
    if (record != null) {
      List<ConsumerRecord<K, V>> list = _processedRecords.computeIfAbsent(tp, k -> new ArrayList<>());
      list.add(record);
    }
  }

  void recordException(TopicPartition tp, long offset, RuntimeException e) {
    _exception = e;
    // The resume offset is the error offset + 1. i.e. if user ignore the exception thrown and poll again, the resuming
    // offset should be this one.
    _resumeOffsets.putIfAbsent(tp, offset + 1);
  }

  boolean hasError(TopicPartition tp) {
    return resumeOffsets().containsKey(tp);
  }

  public RuntimeException exception() {
    return _exception;
  }

  public ConsumerRecords<K, V> consumerRecords() {
    return new ConsumerRecords<>(_processedRecords);
  }

  public Map<TopicPartition, Long> resumeOffsets() {
    return _resumeOffsets;
  }
}
