/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kafka.clients.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AbstractIterator;


/**
 * A container that holds the list {@link ExtendedConsumerRecord} per partition for a
 * particular topic. There is one {@link ExtendedConsumerRecord} list for every topic
 * partition returned by a {@link LiKafkaConsumer#pollX(long)} operation.
 */
public class ExtendedConsumerRecords<K, V> implements Iterable<ExtendedConsumerRecord<K, V>> {

  @SuppressWarnings("unchecked")
  public static final ExtendedConsumerRecords<Object, Object> EMPTY =
      new ExtendedConsumerRecords<>(Collections.EMPTY_MAP);

  private final Map<TopicPartition, List<ExtendedConsumerRecord<K, V>>> records;

  public ExtendedConsumerRecords(Map<TopicPartition, List<ExtendedConsumerRecord<K, V>>> records) {
    this.records = records;
  }

  /**
   * Get just the records for the given partition
   *
   * @param partition The partition to get records for
   */
  public List<ExtendedConsumerRecord<K, V>> records(TopicPartition partition) {
    List<ExtendedConsumerRecord<K, V>> recs = this.records.get(partition);
    if (recs == null) {
      return Collections.emptyList();
    } else {
      return Collections.unmodifiableList(recs);
    }
  }

  /**
   * Get just the records for the given topic
   */
  public Iterable<ExtendedConsumerRecord<K, V>> records(String topic) {
    if (topic == null) {
      throw new IllegalArgumentException("Topic must be non-null.");
    }
    List<List<ExtendedConsumerRecord<K, V>>> recs = new ArrayList<>();
    for (Map.Entry<TopicPartition, List<ExtendedConsumerRecord<K, V>>> entry : records.entrySet()) {
      if (entry.getKey().topic().equals(topic)) {
        recs.add(entry.getValue());
      }
    }
    return new ConcatenatedIterable<K, V>(recs);
  }

  /**
   * Get the partitions which have records contained in this record set.
   * @return the set of partitions with data in this record set (may be empty if no data was returned)
   */
  public Set<TopicPartition> partitions() {
    return Collections.unmodifiableSet(records.keySet());
  }

  @Override
  public Iterator<ExtendedConsumerRecord<K, V>> iterator() {
    return new ConcatenatedIterable<>(records.values()).iterator();
  }

  /**
   * The number of records for all topics
   */
  public int count() {
    int count = 0;
    for (List<ExtendedConsumerRecord<K, V>> recs: this.records.values()) {
      count += recs.size();
    }
    return count;
  }

  private static class ConcatenatedIterable<K, V> implements Iterable<ExtendedConsumerRecord<K, V>> {

    private final Iterable<? extends Iterable<ExtendedConsumerRecord<K, V>>> iterables;

    public ConcatenatedIterable(Iterable<? extends Iterable<ExtendedConsumerRecord<K, V>>> iterables) {
      this.iterables = iterables;
    }

    @Override
    public Iterator<ExtendedConsumerRecord<K, V>> iterator() {
      return new AbstractIterator<ExtendedConsumerRecord<K, V>>() {
        Iterator<? extends Iterable<ExtendedConsumerRecord<K, V>>> iters = iterables.iterator();
        Iterator<ExtendedConsumerRecord<K, V>> current;

        public ExtendedConsumerRecord<K, V> makeNext() {
          while (current == null || !current.hasNext()) {
            if (iters.hasNext()) {
              current = iters.next().iterator();
            } else {
              return allDone();
            }
          }
          return current.next();
        }
      };
    }
  }

  public boolean isEmpty() {
    return records.isEmpty();
  }

  @SuppressWarnings("unchecked")
  public static <K, V> ExtendedConsumerRecords<K, V> empty() {
    return (ExtendedConsumerRecords<K, V>) EMPTY;
  }

}
