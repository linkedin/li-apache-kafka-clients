/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

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
}
