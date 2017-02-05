/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.utils.HeaderKeySpace;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * <p>
 * This class adds the capability to get and set arbitrary header fields which are delivered to the consumer.  The
 * consumer must be using {@link com.linkedin.kafka.clients.consumer.LiKafkaConsumer}.  If headers are present and  the
 * consumer is not using
 * {@link com.linkedin.kafka.clients.consumer.LiKafkaConsumer} then the consumer records will not be deserializable.
 * True null values can not be sent in the presence of any headers.
 * </p>
 */
public class ExtensibleProducerRecord<K, V> extends ProducerRecord<K, V> {

  private Map<Integer, byte[]> headers;

  /**
   * Creates a record with a specified timestamp to be sent to a specified topic and partition
   *
   * TODO: enforce some max header size?
   * TODO: how do the headers count against the message size?
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
   *
   * @param headerKey
   * @return returns null if this record does not have headers of the header is not present
   */
  public byte[] header(int headerKey) {
    HeaderKeySpace.validateHeaderKey(headerKey);

    if (headers == null) {
      return null;
    }
    return headers.get(headerKey);
  }

  /**
   * TODO: enforce some max header size?
   * TODO: how do the headers count against the message size?
   * Adds or updates the headers associated with this record.
   * @param headerKey
   * @param headerValue
   */
  public void header(int headerKey, byte[] headerValue) {
    HeaderKeySpace.validateHeaderKey(headerKey);

    if (headerValue == null) {
      throw new IllegalArgumentException("Header value must not be null.");
    }

    if (headers == null) {
      //Can't use lazy header map because it can't compute hashCode() because it may contain duplicates.
      headers = new TreeMap<>();
    }
    headers.put(headerKey, headerValue);
  }

  public void copyHeadersFrom(ExtensibleProducerRecord<K, V> other) {
    if (other.headers != null) {
      this.headers = new TreeMap<>(other.headers); //TODO: copy on write?
    }
  }

  public Iterator<Integer> headerKeys() {
    if (headers == null) {
      return Collections.EMPTY_LIST.iterator();
    }

    return headers.keySet().iterator();
  }

  /**
   *
   * @return if this record contains any header key value pairs
   */
  public boolean hasHeaders() {
    return headers != null && !headers.isEmpty();
  }

  /**
   *
   * @return this may return null
   */
  Map<Integer, byte[]> headers() {
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

