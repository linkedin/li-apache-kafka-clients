/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;

import com.linkedin.kafka.clients.producer.ExtensibleProducerRecord;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.common.record.TimestampType;

import static org.testng.Assert.assertEquals;

/**
 * The util class for unit test.
 */
public class TestUtils {

  private TestUtils() {
  }

  public static LargeMessageSegment createLargeMessageSegment(UUID messageId,
                                                              int seq,
                                                              int numberOfSegments,
                                                              int messageSizeInBytes,
                                                              int segmentSize) {
    byte[] bytes = new byte[segmentSize];
    Arrays.fill(bytes, (byte) seq);
    return new LargeMessageSegment(messageId, seq, numberOfSegments, messageSizeInBytes, false,  ByteBuffer.wrap(bytes));
  }

  public static void verifyMessage(byte[] serializedMessage, int messageSizeInBytes, int segmentSize) {
    int i = 0;
    for (; i < messageSizeInBytes / segmentSize; i++) {
      for (int j = 0; j < segmentSize; j++) {
        assertEquals(serializedMessage[i * segmentSize + j], (byte) i, "Byte value should match seq.");
      }
    }
    for (int j = 0; j < messageSizeInBytes % segmentSize; j++) {
      assertEquals(serializedMessage[i * segmentSize + j], (byte) i, "Byte value should match seq.");
    }
  }

  public static String getRandomString(int length) {
    char[] chars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    Random random = new Random();
    StringBuilder stringBuilder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      stringBuilder.append(chars[Math.abs(random.nextInt()) % 16]);
    }
    return stringBuilder.toString();
  }

  public static ExtensibleConsumerRecord<byte[], byte[]> producerRecordToConsumerRecord(ExtensibleProducerRecord<byte[], byte[]> producerRecord,
      long offset, long timestamp, TimestampType timestampType, int serializedKeySize, int serializedValueSize) {

    ExtensibleConsumerRecord<byte[], byte[]> consumerRecord =
        new ExtensibleConsumerRecord<>(producerRecord.topic(), producerRecord.partition(), offset, timestamp,
            timestampType, 0, serializedKeySize, serializedValueSize, producerRecord.key(), producerRecord.value());
    Iterator<Integer> headerKeyIterator = producerRecord.headerKeys();
    while (headerKeyIterator.hasNext()) {
      Integer headerKey = headerKeyIterator.next();
      consumerRecord.header(headerKey, producerRecord.header(headerKey));
    }
    try {
      Field headerSizeField = ExtensibleConsumerRecord.class.getDeclaredField("headersReceivedSizeBytes");
      headerSizeField.setAccessible(true);
      headerSizeField.setInt(consumerRecord, producerRecord.headersSize());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return consumerRecord;
  }
}
