/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.header.Headers;


/**
 * Message splitter for large messages
 */
public interface MessageSplitter {

  /**
   * Split the large message into several {@link org.apache.kafka.clients.producer.ProducerRecord}
   * If the IndexedRecord has a GUID, it will be used as key for segment messages to make sure all the segments
   * goes to the same partition.
   *
   * @param topic            The topic to send message to.
   * @param messageId        the message id of this large message. The message id is used to group all the segments of
   *                         this large message.
   * @param serializedRecord the serialized bytes of large message to split
   * @return A list of IndexedRecord each contains a chunk of the original large message.
   */
  List<ProducerRecord<byte[], byte[]>> split(String topic, UUID messageId, byte[] serializedRecord);

  /**
   * Split the large message into several {@link org.apache.kafka.clients.producer.ProducerRecord}
   *
   * @param topic            the topic to send the message to
   * @param key              The key associated with the message. The key will determine witch partition the message goes to.
   *                         If key is null, this method is equivalent to {@link #split(String, UUID, byte[])}
   * @param messageId        the message id of this large message. The message id is used to group all the segments of
   *                         this large message.
   * @param serializedRecord the serialized bytes of large message to split
   * @return A list of IndexedRecord each contains a chunk of the original large message.
   */
  List<ProducerRecord<byte[], byte[]>> split(String topic, UUID messageId, byte[] key, byte[] serializedRecord);

  /**
   * Split the large message into several {@link org.apache.kafka.clients.producer.ProducerRecord}
   *
   * @param topic            the topic to send the message to
   * @param partition        The partition to send the message to. If partition is negative, this method is equivalent to
   *                         {@link #split(String, UUID, byte[])}
   * @param messageId        the message id of this large message. The message id is used to group all the segments of
   *                         this large message.
   * @param serializedRecord the serialized bytes of large message to split
   * @return A list of IndexedRecord each contains a chunk of the original large message.
   */
  List<ProducerRecord<byte[], byte[]>> split(String topic, Integer partition, UUID messageId, byte[] serializedRecord);

  /**
   * Split the large message into several {@link org.apache.kafka.clients.producer.ProducerRecord}
   * If both partition and key are specified, the partition to send the message segments is determined by partition
   * parameter. Otherwise it is determined by key.
   *
   * @param topic            the topic to send the message to.
   * @param partition        The partition to send the message to.
   * @param messageId        the message id of this large message. The message id is used to group all the segments of
   *                         this large message.
   * @param key              The key associated with the message.
   * @param serializedRecord the serialized bytes of large message to split
   * @return A list of IndexedRecord each contains a chunk of the original large message.
   */
  List<ProducerRecord<byte[], byte[]>> split(String topic,
                                             Integer partition,
                                             UUID messageId,
                                             byte[] key,
                                             byte[] serializedRecord);

  /**
   * Split the large message into several {@link org.apache.kafka.clients.producer.ProducerRecord}
   * If both partition and key are specified, the partition to send the message segments is determined by partition
   * parameter. Otherwise it is determined by key.
   *
   * @param topic            the topic to send the message to.
   * @param partition        The partition to send the message to.
   * @param timestamp        The timestamp of the message.
   * @param messageId        the message id of this large message. The message id is used to group all the segments of
   *                         this large message.
   * @param key              The key associated with the message.
   * @param serializedRecord the serialized bytes of large message to split
   * @param headers          headers for the producer record.
   *                         If the header is null, a new record headers is created and large message specific values are added.
   *                         If the header is not null, any old large message keys are removed and new values as generated by the
   *                         current #split() call will be added.
   * @return A list of IndexedRecord each contains a chunk of the original large message.
   */
  List<ProducerRecord<byte[], byte[]>> split(String topic,
                                             Integer partition,
                                             Long timestamp,
                                             UUID messageId,
                                             byte[] key,
                                             byte[] serializedRecord,
                                             Headers headers);

  /**
   * Split the large message into several {@link org.apache.kafka.clients.producer.ProducerRecord}
   * If both partition and key are specified, the partition to send the message segments is determined by partition
   * parameter. Otherwise it is determined by key.
   *
   * @param topic            the topic to send the message to.
   * @param partition        The partition to send the message to.
   * @param timestamp        The timestamp of the message.
   * @param messageId        the message id of this large message. The message id is used to group all the segments of
   *                         this large message.
   * @param key              The key associated with the message.
   * @param serializedRecord the serialized bytes of large message to split
   * @param maxSegmentSize   the max segment size to use to split the message
   * @param headers          headers for the producer record.
`  *                         If the header is null, a new record headers is created and large message specific values are added.
   *                         If the header is not null, any old large message keys are removed and new values as generated by the
   *                         current #split() call will be added.
   * @return A list of IndexedRecord each contains a chunk of the original large message.
   */
  List<ProducerRecord<byte[], byte[]>> split(String topic,
                                             Integer partition,
                                             Long timestamp,
                                             UUID messageId,
                                             byte[] key,
                                             byte[] serializedRecord,
                                             int maxSegmentSize,
                                             Headers headers);

}