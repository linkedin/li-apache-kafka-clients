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
 */
public class ExtendedConsumerRecord<K, V> {
  //TODO: this means all the byte buffers in the consumer record hang around rather than be garbage collected.
  private final ConsumerRecord kafkaConsumerRecord;
  //TODO:  the current LiKafka clients wrap KafkaClient<byte[], byte[]> so we need to have K here.
  private final int serializedKeySize;
  private final K key;
  private final int serializedValueSize;
  private final V value;
  private volatile Map<Integer, ByteBuffer> headers;

  /**
   *
   * @param kafkaConsumerRecord non-null
   * @param key this is the key created after stripping out any additional material the LI Kafka clients have added.
   *            this may be null
   * @param value this is value created after stripping out any additional material the LI Kafka clients have addded.
   *              this may not ne null
   * @param serializedValueSize the size of the user payload in bytes
   * @param headers this may be null
   */
  public ExtendedConsumerRecord(ConsumerRecord<?, ?> kafkaConsumerRecord, K key, int serializedKeySize, V value, int serializedValueSize,
    Map<Integer, ByteBuffer> headers) {

    if (kafkaConsumerRecord == null) {
      throw new IllegalArgumentException("kafkaConsumerRecord must not be null");
    }

    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }

    if (serializedValueSize < 0) {
      throw new IllegalArgumentException("serializedValueSize must be non-negative");
    }

    this.kafkaConsumerRecord = kafkaConsumerRecord;
    this.key = key;
    this.serializedKeySize = serializedKeySize;
    this.value = value;
    this.serializedValueSize = serializedValueSize;
    this.headers = headers;
  }

  /**
   * TODO:  should we allow the user to get the headers in the private space.
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

  /**
   * The topic this record is received from
   */
  public String topic() {
    return this.kafkaConsumerRecord.topic();
  }

  /**
   * The partition from which this record is received
   */
  public int partition() {
    return this.kafkaConsumerRecord.partition();
  }

  /**
   * The key (or null if no key is specified)
   */
  public K key() {
    return this.key;
  }

  /**
   * The value
   */
  public V value() {
    return this.value;
  }

  /**
   * The position of this record in the corresponding Kafka partition.
   */
  public long offset() {
    return kafkaConsumerRecord.offset();
  }

  /**
   * The timestamp of this record
   */
  public long timestamp() {
    return kafkaConsumerRecord.timestamp();
  }

  /**
   * The timestamp type of this record
   */
  public TimestampType timestampType() {
    return kafkaConsumerRecord.timestampType();
  }

  /**
   * The checksum (CRC32) of the record.
   * TODO:  Why is this here?  Can the user actually verify this anyway?  They don't know the message format.
   */
  public long checksum() {
    return this.kafkaConsumerRecord.checksum();
  }

  /**
   * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
   * is -1.
   */
  public int serializedKeySize() {
    return this.kafkaConsumerRecord.serializedKeySize();
  }

  /**
   * The size of the serialized, uncompressed value in bytes. If value is null, the
   * returned size is -1.
   */
  public int serializedValueSize() {
    return this.serializedValueSize;
  }

  //TODO: printout header in some user friendly format
  @Override
  public String toString() {
    return "ExtendedConsumerRecord(topic = " + topic() + ", partition = " + partition() + ", offset = " + offset()
        + ", " + kafkaConsumerRecord.timestampType() + " = " + kafkaConsumerRecord.timestamp() + ", checksum = " + kafkaConsumerRecord.checksum()
        + ", serialized key size = "  + serializedKeySize
        + ", serialized value size = " + serializedValueSize
        + ", key = " + key + ", value = " + value + ")";
  }
}
