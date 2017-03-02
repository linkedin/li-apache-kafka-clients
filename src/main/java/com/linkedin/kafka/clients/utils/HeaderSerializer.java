/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * Implementations define the wire format for headers.  This exists as an interface to support the use case where users
 * have some existing wire format that needs needs to be dealt with or have some internal requirements on message value
 * format.
 */
@InterfaceStability.Unstable
public interface HeaderSerializer extends Configurable {

  /**
   * The maximum size of the all the serialized headers for a single record.
   */
  static final int MAX_SERIALIZED_HEADER_SIZE = 1_024 * 16;

  /**
   *
   * @param headers If this is non-null then serialized header bytes will be written to dest.  If this is null then
   *                an implementation may still write something like a magic number to dest.
   * @param value   The value bytes to be put with headers. The implementation needs to take care of the case where
   *                value is null. So that the header can set the user value to null when unpacking.
   *                We use byte buffer as value instead of bytes here so that we can avoid unnecessary memory copy
   *                for large message serialization.
   */
  byte[] serializeHeaderWithValue(Map<String, byte[]> headers, ByteBuffer value);
}