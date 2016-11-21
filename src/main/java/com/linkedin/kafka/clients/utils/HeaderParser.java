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
import java.util.Map;


/**
 * Header key and length fields are encoded in a 4TLV format using network byte ordering.
 */
public class HeaderParser {

  /**
   * When this is present in the first 4 bytes of a value this indicates is record value that suports headers.
   */
  public static final int HEADER_VALUE_MAGIC = 0x4c4e4b44;

  /**
   * The maximum size of the all the serialized headers for a single record.
   */
  public static final int MAX_SERIALIZED_HEADER_SIZE = 1024 * 16;

  private HeaderParser() {
    // This does nothing
  }

  /**
   *
   * @param bbuf this modifies position() if true has been returned
   * @return true if the remaining bytes in the byte buffer are headers message
   */
  public static boolean isHeaderMessage(ByteBuffer bbuf) {
    if (bbuf.remaining() < 4) {
      return false;
    }

    boolean isHeaderMessage = bbuf.getInt() == HEADER_VALUE_MAGIC;
    if (!isHeaderMessage) {
      bbuf.position(bbuf.position() - 4);
    }
    return isHeaderMessage;
  }

  /**
   *
   * @param src This should be the value part of the underlying producer or consumer message.
   * @param headerMap non-null, mutable map implementation
   * @return a non-null map of key-value pairs.
   */
  public static Map<Integer, byte[]> parseHeader(ByteBuffer src, Map<Integer, byte[]> headerMap) {
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
  public static void writeHeader(ByteBuffer dest, Map<Integer, byte[]> headers) {
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
   * The serialized size of the header.
   * @return 0 if headers is null else the number of bytes needed to represent the header key and value.
   */
  public static int serializedHeaderSize(Map<Integer, byte[]> headers) {
    if (headers == null) {
      return 0;
    }
    int size = headers.size() * 8;
    for (byte[] headerValue : headers.values()) {
      size += headerValue.length;
    }
    return size;
  }

}
