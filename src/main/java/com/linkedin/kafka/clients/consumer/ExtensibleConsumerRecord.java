/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.utils.HeaderKeySpace;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * <p>
 * This extends the Kafka ConsumerRecord with headers which are a set of key-value pairs that can be associated with a
 * record.  The keys are Integer, the values are byte[]. Header key, value pairs are nominally set on the
 * {@link com.linkedin.kafka.clients.producer.ExtensibleProducerRecord}, but can also set after calling
 * {@link org.apache.kafka.clients.consumer.Consumer#poll(long)} by calling {@link #header(int, byte[])}.
 * {@link com.linkedin.kafka.clients.utils.HeaderKeySpace} contains suggestions for how to partition the header key into
 * intervals.  Header keys must be non-negative.  Headers live in a separate space from the underlying Kafka protocol.
 * </p>
 */
public class ExtensibleConsumerRecord<K, V> extends ConsumerRecord<K, V> {
  private volatile Map<Integer, byte[]> headers;

  public ExtensibleConsumerRecord(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
      long checksum, int serializedKeySize, int serializedValueSize, K key, V value) {
    super(topic, partition, offset, timestamp, timestampType, checksum, serializedKeySize, serializedValueSize, key, value);
  }

  ExtensibleConsumerRecord(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
      long checksum, int serializedKeySize, int serializedValueSize, K key, V value, Map<Integer, byte[]> headers) {
    super(topic, partition, offset, timestamp, timestampType, checksum, serializedKeySize, serializedValueSize, key, value);
    this.headers = headers;
  }

  /**
   * Get the header value associated with the specified header key.  This method is not thread safe.
   * @param headerKey non-negative
   * @return returns null if the headerKey does not exist or if this record does not have have headers.
   */
  public byte[] header(int headerKey) {
    if (headers == null) {
      return null;
    }

    return headers.get(headerKey);
  }

  /**
   * This is here so that consumers can set a header key-value pair if it was not set on the producer side.  If the header
   * exists already then the header value is updated.  This method is not thread safe.
   * @param headerKey non-negative
   * @param value non-null
   */
  public void header(int headerKey, byte[] value) {
    HeaderKeySpace.validateHeaderKey(headerKey);

    if (headers == null) {
      headers = new LazyHeaderListMap();
    }

    headers.put(headerKey, value);
  }

  /**
   * Gets the set of all header keys set on this record.  This may not be implemented efficiently.  This method is not
   * thread safe.
   * @return non-null
   */
  public Set<Integer> headerKeys() {
    if (headers == null) {
      return Collections.emptySet();
    }

    return headers.keySet();
  }

  /**
   * Removes the header with the specified key from this consumer record.  This method is not thread safe.
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
   * Copy headers from another record, overriding all headers on this record.  This method is not thread safe.
   * @param other non-null
   */
  public void copyHeadersFrom(ExtensibleConsumerRecord<?, ?> other) {
    //TODO: COW optimization?
    if (other.headers != null) {
      this.headers = new LazyHeaderListMap(other.headers);
    }
  }

  @Override
  public String toString() {
    return "ExtensibleConsumerRecord{header-count=" + (headers == null ? 0 : headers.size()) + " super=" + super.toString() + '}';
  }

}
