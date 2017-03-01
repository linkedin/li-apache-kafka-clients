/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.utils.HeaderUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * <p>
 * This class adds the capability to have key-value pairs, headers, which are delivered to the consumer.  The
 * consumer must be using {@link com.linkedin.kafka.clients.consumer.LiKafkaConsumer}.  If headers are present and the
 * consumer is not using
 * {@link com.linkedin.kafka.clients.consumer.LiKafkaConsumer} then the consumer records will not be deserializable.
 * True null values can not be sent in the presence of any headers.
 * </p>
 * <p>{@link HeaderUtils} contains suggestions for how to partition the header key into
 * intervals.  Header keys must be non-null, non-zero length strings that can be encoded into a utf-8 string.  Headers
 * live in a separate space from the underlying Kafka protocol.
 * </p>
 * <p> A header can be added to a producer record by calling {@link #header(String, byte[])}.  This implies that the
 * user of the producer record should not modify the record after
 * {@link com.linkedin.kafka.clients.producer.LiKafkaProducer#send} has been called.
 * </p>
 */
@InterfaceStability.Unstable
public class ExtensibleProducerRecord<K, V> extends ProducerRecord<K, V> {

  private Map<String, byte[]> headers;

  /**
   * Creates a record with a specified timestamp to be sent to a specified topic and partition
   *
   * @param topic The topic the record will be appended to
   * @param partition The partition to which the record should be sent.  This may be null.
   * @param timestamp The timestamp of the record.  This may be null.
   * @param key The key that will be included in the record.  This may be null.
   * @param value The record contents.  This may be null.
   *
   */
  public ExtensibleProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
    super(topic, partition, timestamp, key, value);
  }

  /**
   * Gets the value associated with the header key from this record.  This method is not thread safe.
   * @param headerKey non-null, non-zero length string that can be encoded into a utf-8 string
   * @return returns null if this record does not have headers of the header is not present
   */
  public byte[] header(String headerKey) {
    HeaderUtils.validateHeaderKey(headerKey);

    if (headers == null) {
      return null;
    }
    return headers.get(headerKey);
  }

  /**
   * Adds or updates the value associated with the header key.  This method is not thread safe.
   * @param headerKey non-null, non-zero length string that can be encoded into a utf-8 string
   * @param headerValue non-null
   */
  public void header(String headerKey, byte[] headerValue) {
    HeaderUtils.validateHeaderKey(headerKey);

    if (headerValue == null) {
      throw new IllegalArgumentException("Header value must not be null.");
    }

    if (headers == null) {
      headers = new HashMap<>();
    }
    headers.put(headerKey, headerValue);
  }

  /**
   * Overrides all the headers on this object if the other object has headers.  This method is not thread safe.
   * @param other non-null
   */
  public void copyHeadersFrom(ExtensibleProducerRecord<K, V> other) {
    if (other.headers != null) {
      this.headers = new HashMap<>(other.headers); //TODO: copy on write?
    }
  }

  /**
   * A set of the header keys currently associated with this record.  This method is not thread safe.
   * @return non-null set of of header keys.
   */
  public Set<String> headerKeys() {
    if (headers == null) {
      return Collections.emptySet();
    }

    return headers.keySet();
  }

  /**
   * A simple predicate that returns true if there are any header key-value mappings on this record.
   * @return true if this record contains any header key value pairs else false
   */
  public boolean hasHeaders() {
    return headers != null && !headers.isEmpty();
  }

  /**
   * This is not public because we don't want to expose the Map interface since this may change to something else in the
   * future.
   * @return this may return null
   */
  Map<String, byte[]> headers() {
    return headers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExtensibleProducerRecord)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ExtensibleProducerRecord<?, ?> that = (ExtensibleProducerRecord<?, ?>) o;

    return headers != null ? headers.equals(that.headers) : that.headers == null;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (headers != null ? headers.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ExtensibleProducerRecord{" + "headers=" + headers + " super=" + super.toString() + '}';
  }
}

