/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.consumer.HeaderKeySpace;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * This mirrors {@link org.apache.kafka.clients.producer.ProducerRecord} in all ways.  This class adds the capability to
 * get and set arbitrary header fields which are delivered to the consumer.  The consumer must be using
 * {@link com.linkedin.kafka.clients.consumer.LiKafkaConsumer}.
 * TODO: more documentation
 * </p>
 */
public class ExtendedProducerRecord<K, V> {

  private final String topic;
  private final Integer partition;
  private final K key;
  private final V value;
  private final Long timestamp;
  private volatile Map<Integer, byte[]> headers;

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
   * @param headers Header key-value pairs.  This may be null if there are not any headers.
   *
   */
  public ExtendedProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Map<Integer, byte[]> headers) {
    if (topic == null) {
      throw new IllegalArgumentException("Topic cannot be null");
    }
    if (timestamp != null && timestamp < 0) {
      throw new IllegalArgumentException("Invalid timestamp " + timestamp);
    }
    if (headers != null) {
      for (Map.Entry<Integer, byte[]> header : headers.entrySet()) {
        if (!HeaderKeySpace.isKeyValid(header.getKey())) {
          throw new IllegalArgumentException("Invalid header key.");
        }
        if (header.getValue() == null) {
          throw new IllegalArgumentException("null header values are not permitted.");
        }
      }
      if (headers.containsKey(HeaderKeySpace.PAYLOAD_HEADER_KEY)) {
        throw new IllegalArgumentException("Can't set the payload key.");
      }
    }
    this.topic = topic;
    this.partition = partition;
    this.key = key;
    this.value = value;
    this.timestamp = timestamp;
    this.headers = headers;
  }

  /**
   *
   * @param headerKey
   * @return returns null if this record does not have headers of the header is not present
   */
  public byte[] header(int headerKey) {
    if (!HeaderKeySpace.isKeyValid(headerKey)) {
      throw new IllegalArgumentException("Invalid header key.");
    }

    return headers.get(headerKey);
  }

  /**
   * TODO: enforce some max header size?
   * TODO: how do the headers count against the message size?
   * TODO: are we supposed to be thread safe?
   * Adds or updates the headers associated with this record.
   * @param headerKey
   * @param headerValue
   */
  public void setHeader(int headerKey, byte[] headerValue) {
    if (!HeaderKeySpace.isKeyValid(headerKey)) {
      throw new IllegalArgumentException("Invalid header key.");
    }
    if (headerValue == null) {
      throw new IllegalArgumentException("Header value must not be null.");
    }

    if (headers == null) {
      headers = new HashMap<>();
    }
    headers.put(headerKey, headerValue);
  }

  /*package private */ Map<Integer, byte[]> headers() {
    return headers;
  }

  /**
   * @return The topic this record is being sent to
   */
  public String topic() {
    return topic;
  }

  /**
   * @return The key (or null if no key is specified)
   */
  public K key() {
    return key;
  }

  /**
   * @return The value
   */
  public V value() {
    return value;
  }

  /**
   * @return The timestamp
   */
  public Long timestamp() {
    return timestamp;
  }

  /**
   * @return The partition to which the record will be sent (or null if no partition was specified)
   */
  public Integer partition() {
    return partition;
  }

  //TODO: print out some header stuff.
  @Override
  public String toString() {
    String key = this.key == null ? "null" : this.key.toString();
    String value = this.value == null ? "null" : this.value.toString();
    String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
    return "ProducerRecord(topic=" + topic + ", partition=" + partition + ", key=" + key + ", value=" + value +
      ", timestamp=" + timestamp + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof ExtendedProducerRecord)) {
      return false;
    }

    ExtendedProducerRecord that = (ExtendedProducerRecord) o;

    if (key != null ? !key.equals(that.key) : that.key != null) {
      return false;
    } else if (partition != null ? !partition.equals(that.partition) : that.partition != null) {
      return false;
    } else if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
      return false;
    } else if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    } else if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) {
      return false;
    } else if (headers != null ? !headers.equals(that.headers) : that.headers != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = topic != null ? topic.hashCode() : 0;
    result = 31 * result + (partition != null ? partition.hashCode() : 0);
    result = 31 * result + (key != null ? key.hashCode() : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
    result = 31 * result + (headers != null ? headers.hashCode() : 0);
    return result;
  }
}

