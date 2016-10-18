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
 * A container that holds the list {@link ExtensibleConsumerRecord} per partition for a
 * particular topic. There is one {@link ExtensibleConsumerRecord} list for every topic
 * partition returned by a {@link LiKafkaConsumer#pollX(long)} operation.
 */
public class ExtensibleConsumerRecords<K, V> implements Iterable<ExtensibleConsumerRecord<K, V>> {

  @SuppressWarnings("unchecked")
  public static final ExtensibleConsumerRecords<Object, Object> EMPTY =
      new ExtensibleConsumerRecords<>(Collections.EMPTY_MAP);

  private final Map<TopicPartition, List<ExtensibleConsumerRecord<K, V>>> records;

  public ExtensibleConsumerRecords(Map<TopicPartition, List<ExtensibleConsumerRecord<K, V>>> records) {
    this.records = records;
  }

  /**
   * Get just the records for the given partition
   *
   * @param partition The partition to get records for
   */
  public List<ExtensibleConsumerRecord<K, V>> records(TopicPartition partition) {
    List<ExtensibleConsumerRecord<K, V>> recs = this.records.get(partition);
    if (recs == null) {
      return Collections.emptyList();
    } else {
      return Collections.unmodifiableList(recs);
    }
  }

  /**
   * Get just the records for the given topic
   */
  public Iterable<ExtensibleConsumerRecord<K, V>> records(String topic) {
    if (topic == null) {
      throw new IllegalArgumentException("Topic must be non-null.");
    }
    List<List<ExtensibleConsumerRecord<K, V>>> recs = new ArrayList<>();
    for (Map.Entry<TopicPartition, List<ExtensibleConsumerRecord<K, V>>> entry : records.entrySet()) {
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
  public Iterator<ExtensibleConsumerRecord<K, V>> iterator() {
    return new ConcatenatedIterable<>(records.values()).iterator();
  }

  /**
   * The number of records for all topics
   */
  public int count() {
    int count = 0;
    for (List<ExtensibleConsumerRecord<K, V>> recs: this.records.values()) {
      count += recs.size();
    }
    return count;
  }

  private static class ConcatenatedIterable<K, V> implements Iterable<ExtensibleConsumerRecord<K, V>> {

    private final Iterable<? extends Iterable<ExtensibleConsumerRecord<K, V>>> iterables;

    public ConcatenatedIterable(Iterable<? extends Iterable<ExtensibleConsumerRecord<K, V>>> iterables) {
      this.iterables = iterables;
    }

    @Override
    public Iterator<ExtensibleConsumerRecord<K, V>> iterator() {
      return new AbstractIterator<ExtensibleConsumerRecord<K, V>>() {
        Iterator<? extends Iterable<ExtensibleConsumerRecord<K, V>>> iters = iterables.iterator();
        Iterator<ExtensibleConsumerRecord<K, V>> current;

        public ExtensibleConsumerRecord<K, V> makeNext() {
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
  public static <K, V> ExtensibleConsumerRecords<K, V> empty() {
    return (ExtensibleConsumerRecords<K, V>) EMPTY;
  }

}
