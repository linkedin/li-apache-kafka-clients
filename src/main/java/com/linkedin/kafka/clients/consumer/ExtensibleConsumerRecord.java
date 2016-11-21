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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * A key/value pair to be received from Kafka. This consists of a topic name and a partition number, from which the
 * record is being received and an offset that points to the record in a Kafka partition.
 * This extension allows for user definable headers.
 */
public class ExtensibleConsumerRecord<K, V> extends ConsumerRecord<K, V> {
  private volatile Map<Integer, byte[]> headers;
  private volatile int headersSize;

  public ExtensibleConsumerRecord(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
      long checksum, int serializedKeySize, int serializedValueSize, K key, V value) {
    super(topic, partition, offset, timestamp, timestampType, checksum, serializedKeySize, serializedValueSize, key, value);
    headersSize = 0;
  }

  ExtensibleConsumerRecord(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
      long checksum, int serializedKeySize, int serializedValueSize, K key, V value, Map<Integer, byte[]> headers, int headersSize) {
    super(topic, partition, offset, timestamp, timestampType, checksum, serializedKeySize, serializedValueSize, key, value);
    this.headers = headers;
    this.headersSize = headersSize;
  }

  /**
   * @param headerKey
   * @return returns null if the headerKey does not exist or if this record does not have have headers.
   */
  public byte[] header(int headerKey) {
    if (headers == null) {
      return null;
    }

    if (!headers.containsKey(headerKey)) {
      return null;
    }

    return headers.get(headerKey);
  }

  /**
   * This is here so that consumers can set some kind of header value if it was not set on the producer.  If the header
   * exists already then the header value is updated.
   * @param headerKey non-negative
   * @param value non-null
   */
  public void header(int headerKey, byte[] value) {
    if (!HeaderKeySpace.isKeyValid(headerKey)) {
      throw new IllegalArgumentException("Header key " + headerKey + " is not valid.");
    }

    if (headers == null) {
      headers = new HashMap<>();
    }

    this.headers.put(headerKey, value);
  }

  public Iterator<Integer> headerKeys() {
    if (headers == null) {
      return Collections.emptyIterator();
    }

    return headers.keySet().iterator();
  }

  /**
   * Removes the header with the specified key from this consumer record.
   * @return The previously mapped value else this returns null.
   */
  public byte[] removeHeader(int headerKey)  {
    if (headers == null) {
      return null;
    }
    return headers.remove(headerKey);
  }

  Map<Integer, byte[]> headers() {
    return headers;
  }

  /**
   * Copy headers from another record, overriding the headers on this record.
   */
  public void copyHeadersFrom(ExtensibleConsumerRecord<?, ?> other) {
    //TODO: COW optimization?
    if (other.headers != null) {
      this.headersSize = other.headersSize;
      this.headers = new LazyHeaderListMap(other.headers);
    }
  }

  public void headersSize(int newHeadersSize) {
    if (newHeadersSize < 0) {
      throw new IllegalArgumentException("newHeadersSize must be a non-negative integer.");
    }
    this.headersSize = newHeadersSize;
  }

  /**
   * This is the size of the headers that were received from the broker.  Headers added or removed after that point are
   * not counted in this value.
   */
  public int headersSize() {
    return headersSize;
  }

  @Override
  public String toString() {
    return "ExtensibleConsumerRecord{header-count=" + (headers == null ? 0 : headers.size()) + " super=" + super.toString() + '}';
  }

}
