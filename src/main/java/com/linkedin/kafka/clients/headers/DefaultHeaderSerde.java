/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.headers;

import java.nio.ByteBuffer;


/**
 * Utilities to implement header serializer and deserializer.
 */
public abstract class DefaultHeaderSerde {
  /**
   *   All this bit manipulation makes this number look like an invalid UTF8 encoded string if someone starts to read it
   * within the first 7 bytes
   */
  public static final long DEFAULT_HEADER_MAGIC
    = (0x4c6d4eef4b7a44L | 0b11000000_11000000_11000000_11000000_11000000_11000000_11000000_11000000L) &
    0b11011111_11011111_11011111_11011111_11011111_11011111_11011111_11011111L;

  protected static final int MAGIC_SIZE = 8;

  protected static final byte VERSION_1 = 1;

  protected static int VERSION_AND_FLAGS_SIZE = 1;

  protected static final int ALL_HEADER_SIZE_FIELD_SIZE = 4;

  protected static final byte USER_VALUE_IS_NULL_FLAG = 0x10;

  /**
   * The size of the size field that encoded the key length.
   */
  protected static final int KEY_SIZE_SIZE = 1;

  /**
   * The size of the value size field.
   */
  protected static final int VALUE_SIZE_SIZE = 4;

  /**
   * @return DEFAULT_HEADER_MAGIC as byte array.
   */
  static byte[] defaultHeaderMagicBytes() {
    ByteBuffer bbuf = ByteBuffer.allocate(MAGIC_SIZE);
    bbuf.putLong(DEFAULT_HEADER_MAGIC);
    return bbuf.array();
  }

  /**
   * The length of a utf8 encoded string.
   * @param s non-null
   * @return a non-negative integer. The number of bytes in the utf-8 encoded form of the string
   */
  static int utf8StringLength(String s) {
    int stringLength = s.length();
    int utf8Length = 0;
    for (int i = 0; i < stringLength; i++) {
      char c = s.charAt(i);
      if (c < 0x80) {
        utf8Length++;
      } else if (c < 0x0800) {
        utf8Length += 2;
      } else if (!Character.isHighSurrogate(c)) {
        utf8Length += 3;
      } else {
        //This character is a high surrogate which means the next char also composes the unicode
        //code point (character).  That also means it's in the unicode range starting with U+10000
        //which means it is always encoded with 4 bytes in UTF-8
        utf8Length += 4;
        i++; //consume next character as well.
      }
    }
    return utf8Length;
  }


}
