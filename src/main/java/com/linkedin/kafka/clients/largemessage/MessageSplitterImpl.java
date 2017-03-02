/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.headers.HeaderUtils;
import com.linkedin.kafka.clients.producer.ExtensibleProducerRecord;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.UUIDFactory;
import java.util.Collection;
import java.util.Collections;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This class is used by {@link com.linkedin.kafka.clients.producer.LiKafkaProducerImpl} to split
 * serialized records.
 */
public class MessageSplitterImpl implements MessageSplitter {
  // This class does not do anything with the original record, so no key serializer is needed.
  private final int _maxSegmentSize;
  private final UUIDFactory _uuidFactory;

  public MessageSplitterImpl(int maxSegmentSize, UUIDFactory uuidFactory) {
    if (maxSegmentSize <= 0) {
      throw new IllegalArgumentException("maxSegmentSize must be a positive integer.");
    }
    if (uuidFactory == null) {
      throw new IllegalArgumentException("uuidFactory must not be null");
    }
    this._maxSegmentSize = maxSegmentSize;
    this._uuidFactory = uuidFactory;
  }

  @Override
  public Collection<ExtensibleProducerRecord<byte[], byte[]>> split(ExtensibleProducerRecord<byte[], byte[]> originalRecord) {
    int messageSizeInBytes = originalRecord.value() == null ? 0 : originalRecord.value().length;
    if (messageSizeInBytes < _maxSegmentSize) {
      return Collections.singleton(originalRecord);
    }

    UUID segmentMessageId = _uuidFactory.create();
    // Get the total number of segments
    int numberOfSegments = (messageSizeInBytes + (_maxSegmentSize - 1)) / _maxSegmentSize;
    List<ExtensibleProducerRecord<byte[], byte[]>> segments = new ArrayList<>(numberOfSegments);

    ByteBuffer bytebuffer = ByteBuffer.wrap(originalRecord.value());

    byte[] key = originalRecord.key();
    //If we don't set a key then mirror maker can scatter the message segments to the wind.
    if (key == null) {
      key = LiKafkaClientsUtils.uuidToBytes(segmentMessageId);
    }

    // Sequence number starts from 0.
    for (int seq = 0; seq < numberOfSegments; seq++) {
      int segmentStart = seq * _maxSegmentSize;
      int segmentLength = Math.min(originalRecord.value().length - segmentStart, _maxSegmentSize);
      bytebuffer.position(segmentStart);
      ByteBuffer payload = bytebuffer.slice();
      payload.limit(segmentLength);
      LargeMessageSegment segment = new LargeMessageSegment(segmentMessageId, seq,
          numberOfSegments, messageSizeInBytes, originalRecord.key() == null, payload);

      ExtensibleProducerRecord<byte[], byte[]> segmentProducerRecord =
        new ExtensibleProducerRecord<>(originalRecord.topic(), originalRecord.partition(), originalRecord.timestamp(), key, segment.segmentArray());
      segmentProducerRecord.copyHeadersFrom(originalRecord);
      segmentProducerRecord.header(HeaderUtils.LARGE_MESSAGE_SEGMENT_HEADER, segment.segmentHeader());
      segments.add(segmentProducerRecord);
    }

    return segments;
  }
}
