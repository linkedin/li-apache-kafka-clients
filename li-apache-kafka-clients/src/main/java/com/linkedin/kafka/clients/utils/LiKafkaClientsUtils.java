/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Util class for likafka-clients.
 */
public class LiKafkaClientsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaClientsUtils.class);

  private static SecureRandom _secureRandom;

  static {
    try {
      _secureRandom = SecureRandom.getInstance("SHA1PRNG");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Cannot get random UUID due to", e);
    }
  }

  private LiKafkaClientsUtils() {
  }

  public static UUID randomUUID() {
    byte[] bytes = new byte[16];
    _secureRandom.nextBytes(bytes);
    // Set UUID version number 4
    bytes[6] &= 0x0f;
    bytes[6] |= 0x40;
    // Set IETF variant
    bytes[8] &= 0x3f;
    bytes[8] |= 0x80;
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
  }

  public static byte[] uuidToBytes(UUID uuid) {
    byte[] bytes = new byte[16];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putLong(uuid.getMostSignificantBits());
    byteBuffer.putLong(uuid.getLeastSignificantBits());
    return bytes;
  }

  /**
   * Get the user offset from the metadata of the committed offsets.
   * @param metadata the associated metadata.
   * @return the committed user offset, null if there is no such offset. (i.e. the metadata is not committed by
   *         a LiKafkaConsumer.
   */
  public static Long offsetFromWrappedMetadata(String metadata) {
    // handle the offset committed by raw KafkaConsumers
    if (metadata == null) {
      return null;
    } else {
      int separatorIndex = metadata.indexOf(',');
      if (separatorIndex < 0) {
        return null;
      } else {
        try {
          return Long.parseLong(metadata.substring(0, separatorIndex));
        } catch (NumberFormatException nfe) {
          return null;
        }
      }
    }
  }

  public static String metadataFromWrappedMetadata(String metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return metadata;
    } else {
      return metadata.substring(metadata.indexOf(',') + 1);
    }
  }

  public static String wrapMetadataWithOffset(String metadata, long offset) {
    return Long.toString(offset) + "," + metadata;
  }

  // Dump stack traces of all live threads among the given set of threads.
  public static void dumpStacksForAllLiveThreads(Set<Thread> threads) {
    Set<Thread> allLiveThreads = Thread.getAllStackTraces().keySet();
    LOG.error("currently live threads:");
    for (Thread t : threads) {
      if (allLiveThreads.contains(t)) {
        LOG.error("Thread {} (state: {}):", t, t.getState());
        t.dumpStack();
      }
    }
  }

  // Java 9's TimeUnit#toChronoUnit() will replace this.
  public static ChronoUnit convertTimeUnitToChronoUnit(TimeUnit timeUnit) {
    switch (timeUnit) {
      case DAYS:
        return ChronoUnit.DAYS;
      case HOURS:
        return ChronoUnit.HOURS;
      case MICROSECONDS:
        return ChronoUnit.MICROS;
      case MILLISECONDS:
        return ChronoUnit.MILLIS;
      case MINUTES:
        return ChronoUnit.MINUTES;
      case NANOSECONDS:
        return ChronoUnit.NANOS;
      case SECONDS:
        return ChronoUnit.SECONDS;
      default:
        throw new IllegalArgumentException("Invalid TimeUnit value: " + timeUnit);
    }
  }
}
