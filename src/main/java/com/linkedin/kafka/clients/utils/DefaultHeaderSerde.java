/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.utils;

import java.nio.ByteBuffer;


/**
 * Utilities to implement header Serde.
 */
public interface DefaultHeaderSerde {
  /**
   *   All this bit manipulation makes this number look like an invalid UTF8 encoded string if someone starts to read it
   * within the first 7 bytes
   */
  long DEFAULT_HEADER_MAGIC
    = (0x4c6d4eef4b7a44L | 0b11000000_11000000_11000000_11000000_11000000_11000000_11000000_11000000L) &
    0b11011111_11011111_11011111_11011111_11011111_11011111_11011111_11011111L;


  byte VERSION_1 = 1;

  int VERSION_AND_FLAGS_SIZE = 1;

  int ALL_HEADER_SIZE_FIELD_SIZE = 4;

  byte USER_VALUE_IS_NULL_FLAG = 0x10;

  /**
   * @return DEFAULT_HEADER_MAGIC as byte array.
   */
  static byte[] defaultHeaderMagicBytes() {
    ByteBuffer bbuf = ByteBuffer.allocate(8);
    bbuf.putLong(DEFAULT_HEADER_MAGIC);
    return bbuf.array();
  }


}
