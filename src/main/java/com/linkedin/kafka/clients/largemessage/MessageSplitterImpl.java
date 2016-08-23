/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This class is used by {@link com.linkedin.kafka.clients.producer.LiKafkaProducerImpl} to split to split
 * serialized records.
 */
public class MessageSplitterImpl implements MessageSplitter {
  // This class does not do anything with the original record, so no key serializer is needed.
  private final int _maxSegmentSize;
  private final Serializer<LargeMessageSegment> _segmentSerializer;

  public MessageSplitterImpl(int maxSegmentSize,
                             Serializer<LargeMessageSegment> segmentSerializer) {
    _maxSegmentSize = maxSegmentSize;
    _segmentSerializer = segmentSerializer;
  }

  @Override
  public List<ProducerRecord<byte[], byte[]>> split(String topic, UUID messageId, byte[] serializedRecord) {
    return split(topic, messageId, null, serializedRecord);
  }

  @Override
  public List<ProducerRecord<byte[], byte[]>> split(String topic, UUID messageId, byte[] key, byte[] serializedRecord) {
    return split(topic, null, messageId, key, serializedRecord);
  }

  @Override
  public List<ProducerRecord<byte[], byte[]>> split(String topic, Integer partition, UUID messageId, byte[] serializedRecord) {
    return split(topic, partition, messageId, null, serializedRecord);
  }

  @Override
  public List<ProducerRecord<byte[], byte[]>> split(String topic, Integer partition, UUID messageId, byte[] key, byte[] serializedRecord) {
    return split(topic, partition, null, messageId, key, serializedRecord);
  }

  @Override
  public List<ProducerRecord<byte[], byte[]>> split(String topic, Integer partition, Long timestamp, UUID messageId, byte[] key, byte[] serializedRecord) {
    return split(topic, partition, timestamp, messageId, key, serializedRecord, _maxSegmentSize);
  }

  @Override
  public List<ProducerRecord<byte[], byte[]>> split(String topic,
                                                    Integer partition,
                                                    Long timestamp,
                                                    UUID messageId,
                                                    byte[] key,
                                                    byte[] serializedRecord,
                                                    int maxSegmentSize) {

    if (topic == null) {
      throw new IllegalArgumentException("Topic cannot be empty for LiKafkaGenericMessageSplitter.");
    }
    // We allow message id to be null, but it is strongly recommended to pass in a message id.
    UUID segmentMessageId = messageId == null ? UUID.randomUUID() : messageId;
    List<ProducerRecord<byte[], byte[]>> segments = new ArrayList<>();
    // Get the total number of segments
    int numberOfSegments = (serializedRecord.length + (maxSegmentSize - 1)) / maxSegmentSize;
    // Get original message size in bytes
    int messageSizeInBytes = serializedRecord.length;
    ByteBuffer bytebuffer = ByteBuffer.wrap(serializedRecord);

    byte[] segmentKey = key == null ? LiKafkaClientsUtils.uuidToBytes(segmentMessageId) : key;
    // Sequence number starts from 0.
    for (int seq = 0; seq < numberOfSegments; seq++) {
      int segmentStart = seq * maxSegmentSize;
      int segmentLength = Math.min(serializedRecord.length - segmentStart, maxSegmentSize);
      // For efficiency we do not make array copy, but just slice the ByteBuffer. The segment serializer needs to
      // decide how to deal with the payload ByteBuffer.
      bytebuffer.position(segmentStart);
      ByteBuffer payload = bytebuffer.slice();
      payload.limit(segmentLength);
      LargeMessageSegment segment = new LargeMessageSegment(segmentMessageId, seq,
          numberOfSegments, messageSizeInBytes, payload);

      // NOTE: we have to use null topic here to serialize because the segment should be topic independent.
      byte[] segmentValue = _segmentSerializer.serialize(null, segment);
      ProducerRecord<byte[], byte[]> segmentProducerRecord =
          new ProducerRecord<>(topic, partition, timestamp, segmentKey, segmentValue);
      segments.add(segmentProducerRecord);
    }

    return segments;
  }
}
