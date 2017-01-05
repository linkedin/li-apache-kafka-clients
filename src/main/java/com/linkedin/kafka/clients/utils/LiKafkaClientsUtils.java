/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Util class for likafka-clients.
 */
public class LiKafkaClientsUtils {

  private LiKafkaClientsUtils() {
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

}
