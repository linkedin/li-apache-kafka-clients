/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import static org.testng.Assert.assertEquals;

/**
 * The util class for unit test.
 */
public class LiKafkaClientsTestUtils {

  private LiKafkaClientsTestUtils() {
  }

  public static LargeMessageSegment createLargeMessageSegment(UUID messageId,
      int seq,
      int numberOfSegments,
      int messageSizeInBytes,
      int segmentSize) {
    byte[] bytes = new byte[segmentSize];
    Arrays.fill(bytes, (byte) seq);
    return new LargeMessageSegment(messageId, seq, numberOfSegments, messageSizeInBytes, ByteBuffer.wrap(bytes));
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
    StringBuilder stringBuiler = new StringBuilder();
    for (int i = 0; i < length; i++) {
      stringBuiler.append(chars[Math.abs(random.nextInt()) % 16]);
    }
    return stringBuiler.toString();
  }

  /**
   * Special header keys have a "_" prefix and are managed internally by the clients.
   * @param headers
   * @return
   */
  public static Map<String, byte[]> fetchSpecialHeaders(Headers headers) {
    Map<String, byte[]> map = new HashMap<>();
    for (Header header : headers) {

      if (!header.key().startsWith("_")) {
        // skip any non special header
        continue;
      }

      if (map.containsKey(header.key())) {
        throw new IllegalStateException("Duplicate special header found " + header.key());
      }
      map.put(header.key(), header.value());
    }
    return map;
  }
}
