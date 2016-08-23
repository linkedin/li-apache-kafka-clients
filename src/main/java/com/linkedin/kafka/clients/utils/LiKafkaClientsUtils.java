/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

  public static long offsetFromWrappedMetadata(String metadata) {
    return Long.parseLong(metadata.substring(0, metadata.indexOf(',')));
  }

  public static String metadataFromWrappedMetadata(String metadata) {
    return metadata.substring(metadata.indexOf(',') + 1);
  }

  public static String wrapMetadataWithOffset(String metadata, long offset) {
    return Long.toString(offset) + "," + metadata;
  }

}
