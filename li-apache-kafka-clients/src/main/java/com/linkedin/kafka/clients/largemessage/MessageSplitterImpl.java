/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.common.LargeMessageHeaderValue;
import com.linkedin.kafka.clients.utils.Constants;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.producer.UUIDFactory;
import java.util.Collections;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
  private final UUIDFactory _uuidFactory;
  private final boolean _enableRecordHeader;

  public MessageSplitterImpl(int maxSegmentSize,
                             Serializer<LargeMessageSegment> segmentSerializer,
                             UUIDFactory uuidFactory) {
    this(maxSegmentSize, segmentSerializer, uuidFactory, false);
  }

  public MessageSplitterImpl(int maxSegmentSize,
      Serializer<LargeMessageSegment> segmentSerializer,
      UUIDFactory uuidFactory, boolean enableRecordHeader) {
    _maxSegmentSize = maxSegmentSize;
    _segmentSerializer = segmentSerializer;
    _uuidFactory = uuidFactory;
    _enableRecordHeader = enableRecordHeader;
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
  @Deprecated
  public List<ProducerRecord<byte[], byte[]>> split(String topic, Integer partition, UUID messageId, byte[] key, byte[] serializedRecord) {
    return split(topic, partition, null, messageId, key, serializedRecord, null);
  }

  @Override
  public List<ProducerRecord<byte[], byte[]>> split(String topic, Integer partition,
      Long timestamp, UUID messageId, byte[] key, byte[] serializedRecord, Headers headers) {
    return split(topic, partition, timestamp, messageId, key, serializedRecord, _maxSegmentSize, headers);
  }

  @Override
  public List<ProducerRecord<byte[], byte[]>> split(String topic,
                                                    Integer partition,
                                                    Long timestamp,
                                                    UUID messageId,
                                                    byte[] key,
                                                    byte[] serializedRecord,
                                                    int maxSegmentSize,
                                                    Headers headers) {
    if (topic == null) {
      throw new IllegalArgumentException("Topic cannot be empty.");
    }
    if (serializedRecord == null || serializedRecord.length == 0) {
      return Collections.singletonList(new ProducerRecord<>(topic, partition, timestamp, key, serializedRecord));
    }
    if (headers == null) {
      // If null, create a new RecordHeaders
      headers = new RecordHeaders();
    }

    // We allow message id to be null, but it is strongly recommended to pass in a message id.
    UUID segmentMessageId = messageId == null ? _uuidFactory.createUuid() : messageId;
    List<ProducerRecord<byte[], byte[]>> segments = new ArrayList<>();
    // Get the total number of segments
    int numberOfSegments = (serializedRecord.length + (maxSegmentSize - 1)) / maxSegmentSize;
    // Get original message size in bytes
    int messageSizeInBytes = serializedRecord.length;
    ByteBuffer bytebuffer = ByteBuffer.wrap(serializedRecord);
    //messages with >1 segments absolutely must have a != null key set to guarantee they land in the same partition
    byte[] segmentKey = (key == null && numberOfSegments > 1) ? LiKafkaClientsUtils.uuidToBytes(segmentMessageId) : key;
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

      byte[] segmentValue;
      byte largeMessageHeaderValueVersion;
      if (_enableRecordHeader) {
        segmentValue = payload.array();
        largeMessageHeaderValueVersion = LargeMessageHeaderValue.V3;
      } else {
        // NOTE: Even though we are passing topic here to serialize, the segment itself should be topic independent.
        segmentValue = _segmentSerializer.serialize(topic, segment);
        largeMessageHeaderValueVersion = LargeMessageHeaderValue.LEGACY_V2;
      }

      //  Make a temporary copy of headers because we'd be overwriting {@link Constants.LARGE_MESSAGE_HEADER}
      Headers temporaryHeaders = new RecordHeaders(headers);
      temporaryHeaders.remove(Constants.LARGE_MESSAGE_HEADER);

      LargeMessageHeaderValue largeMessageHeaderValue = new LargeMessageHeaderValue(
          largeMessageHeaderValueVersion,
          messageId,
          seq,
          numberOfSegments,
          messageSizeInBytes);
      temporaryHeaders.add(Constants.LARGE_MESSAGE_HEADER, LargeMessageHeaderValue.toBytes(largeMessageHeaderValue));
      ProducerRecord<byte[], byte[]> segmentProducerRecord =
          new ProducerRecord<>(
              topic,
              partition,
              timestamp,
              segmentKey,
              segmentValue,
              temporaryHeaders
          );
      segments.add(segmentProducerRecord);
    }

    return segments;
  }
}
