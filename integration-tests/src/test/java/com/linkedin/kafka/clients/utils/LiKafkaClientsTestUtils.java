/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.kafka.clients.CommonClientConfigs.*;
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

  public static Properties buildConsumerProps() {
    Properties props = new Properties();
    props.setProperty(CLIENT_ID_CONFIG, "testLargeMessageConsumer");
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testLargeMessageConsumer");
    props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
    props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "blah");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
    props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");
    props.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "30000");
    props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576");
    props.setProperty(ConsumerConfig.SEND_BUFFER_CONFIG, "131072");
    props.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "32768");
    props.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "50");
    props.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "6000");
    props.setProperty(ConsumerConfig.CHECK_CRCS_CONFIG, "true");
    props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor");
    return props;
  }
}
