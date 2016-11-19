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

import com.linkedin.kafka.clients.consumer.HeaderKeySpace;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


/**
 * Header key and length fields are encoded in a 4TLV format using network byte ordering.
 */
public class HeaderParser {

  /**
   * When this is present in the first 8 bytes of a record value this indicates a record value that suports headers.
   */
  private static final long HEADER_MAGIC
    // All this bit manipulation makes this number look like an invalid UTF8 encoded string if someone starts to read it
    // within the first 7 bytes
    = (0x4c6d4eef4b7a44L | 0b11000000_11000000_11000000_11000000_11000000_11000000_11000000_11000000L) &
        0b11011111_11011111_11011111_11011111_11011111_11011111_11011111_11011111L;


  private static final byte[] HEADER_MAGIC_AS_BYTES;
  static {
    ByteBuffer bbuf = ByteBuffer.allocate(8);
    bbuf.putLong(HEADER_MAGIC);
    HEADER_MAGIC_AS_BYTES = bbuf.array();
  }

  private final byte[] headerMagicValue;

  /**
   * The maximum size of the all the serialized headers for a single record.
   */
  public static final int MAX_SERIALIZED_HEADER_SIZE = 1_024 * 16;


  public HeaderParser() {
    this(HEADER_MAGIC_AS_BYTES);
  }

  public HeaderParser(byte[] headerMagicValue) {
    if (headerMagicValue == null) {
      throw new IllegalArgumentException("headerMagicValue must not be null");
    }
    this.headerMagicValue = headerMagicValue;
  }

  /**
   *
   * @return a non negative number
   */
  public int magicLength() {
    return headerMagicValue.length;
  }

  /**
   *
   * @param bbuf this modifies position() if true has been returned
   * @return true if the remaining bytes in the byte buffer are headers message
   */
  public boolean isHeaderMessage(ByteBuffer bbuf) {
    if (bbuf.remaining() < magicLength()) {
      return false;
    }

    boolean isHeaderMessage = true;
    for (int i = 0; i < headerMagicValue.length; i++) {
      isHeaderMessage = isHeaderMessage && headerMagicValue[i] == bbuf.get();
    }
    if (!isHeaderMessage) {
      bbuf.position(bbuf.position() - magicLength());
    }
    return isHeaderMessage;
  }

  /**
   *
   * @param src This should be the value part of the underlying producer or consumer message.
   * @param headerMap non-null, mutable map implementation
   * @return a non-null map of key-value pairs.
   */
  public Map<Integer, byte[]> parseHeader(ByteBuffer src, Map<Integer, byte[]> headerMap) {
    while (src.hasRemaining()) {
      int headerKey = src.getInt();
      if (!HeaderKeySpace.isKeyValid(headerKey)) {
        throw new IllegalArgumentException("Byte buffer contains an invalid header key.");
      }
      int headerValueLength = src.getInt();
      byte[] headerValue = new byte[headerValueLength];
      src.get(headerValue);
      headerMap.put(headerKey, headerValue);
    }

    return headerMap;
  }


  /**
   *
   * @param dest The destination byte buffer where we should write the headers to.  If this method throws an exception
   *             the caller can not assume any particular state of dest.
   * @param headers This writes nothing if headers is null.
   */
  public void writeHeader(ByteBuffer dest, Map<Integer, byte[]> headers) {
    if (headers == null) {
      return;
    }
    for (Map.Entry<Integer, byte[]> header : headers.entrySet()) {
      if (!HeaderKeySpace.isKeyValid(header.getKey())) {
        throw new IllegalArgumentException("Headers contain an invalid key.");
      }
      dest.putInt(header.getKey());
      dest.putInt(header.getValue().length);
      dest.put(header.getValue());
    }
  }

  /**
   * The serialized size of all the headers.
   * @return 0 if headers is null else the number of bytes needed to represent the header key and value.
   */
  public int serializedHeaderSize(Map<Integer, byte[]> headers) {
    if (headers == null) {
      return 0;
    }
    int size = headers.size() * 8; // size of all the keys and the value length fields
    for (byte[] headerValue : headers.values()) {
      size += headerValue.length;
    }
    return size;
  }

}
