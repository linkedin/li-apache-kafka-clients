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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * A key/value pair to be received from Kafka. This consists of a topic name and a partition number, from which the
 * record is being received and an offset that points to the record in a Kafka partition.
 * This extension allows for user definable headers.
 */
public class ExtensibleConsumerRecord<K, V> extends ConsumerRecord<K,V> {
  private volatile Map<Integer, ByteBuffer> headers;

  public ExtensibleConsumerRecord(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
      long checksum, int serializedKeySize, int serializedValueSize, K key, V value, Map<Integer, ByteBuffer> headers) {
    super(topic, partition, offset, timestamp, timestampType, checksum, serializedKeySize, serializedValueSize, key, value);
    this.headers = headers;
  }

  /**
   * TODO:  should we allow the user to get the headers in the private address space.
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

    ByteBuffer value = headers.get(headerKey);
    byte[] copy = new byte[value.remaining()];
    //TODO: consumer is not thread safe, right?
    value.get(copy);
    value.flip();
    return copy;
  }

  /**
   * TODO:  Do we really want this?
   * This is here so that consumers can set some kind of header value if it was not set on the producer.  If the header
   * exists already then the header value is updated.
   * @param headerKey non-negative
   * @param value non-null
   */
  public void setHeader(int headerKey, byte[] value) {
    if (!HeaderKeySpace.isKeyValid(headerKey)) {
      throw new IllegalArgumentException("Key " + headerKey + " is not valid.");
    }
    //TODO:  or do we want this.  Initially I'm against letting people do this. We can always change later.
    if (HeaderKeySpace.isKeyInPrivateRange(headerKey)) {
      throw new IllegalArgumentException(("Key must not be in private range."));
    }

    if (headers == null) {
      headers = new HashMap<>();
    }

    this.headers.put(headerKey, ByteBuffer.wrap(value)); //TODO: wrapped array means value still mutable from outside
  }

  @Override
  public String toString() {
    return "ExtensibleConsumerRecord{" + "headers=" + headers + " super=" + super.toString() + '}';
  }
}
