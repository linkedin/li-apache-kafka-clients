/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.utils;

import java.nio.ByteBuffer;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;


/**
 *  <pre>
 *  8 bytes magic number (default)
 *  1 byte version
 *  Varies Header key and length fields are encoded in a 4TLV format using network byte ordering.
 *  </pre>
 */
public class HeaderParser {

  public static final long DEFAULT_HEADER_MAGIC
    // All this bit manipulation makes this number look like an invalid UTF8 encoded string if someone starts to read it
    // within the first 7 bytes
    = (0x4c6d4eef4b7a44L | 0b11000000_11000000_11000000_11000000_11000000_11000000_11000000_11000000L) &
        0b11011111_11011111_11011111_11011111_11011111_11011111_11011111_11011111L;


  private static final byte VERSION_1 = 1;

  private static final int VERSION_SIZE = 1;

  private static final byte[] DEFAULT_HEADER_MAGIC_AS_BYTES;
  static {
    ByteBuffer bbuf = ByteBuffer.allocate(8);
    bbuf.putLong(DEFAULT_HEADER_MAGIC);
    DEFAULT_HEADER_MAGIC_AS_BYTES = bbuf.array();
  }

  public static String defaultMagicAsString() {
    return DatatypeConverter.printHexBinary(DEFAULT_HEADER_MAGIC_AS_BYTES);
  }

  private final byte[] headerMagicValue;

  /**
   * The maximum size of the all the serialized headers for a single record.
   */
  public static final int MAX_SERIALIZED_HEADER_SIZE = 1_024 * 16;


  public HeaderParser() {
    this(DEFAULT_HEADER_MAGIC_AS_BYTES);
  }

  /**
   *
   * @param hexString just 0-9A-F no 0x prefix
   */
  public HeaderParser(String hexString) {
    this(DatatypeConverter.parseHexBinary(hexString));
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
  public int magicSize() {
    return headerMagicValue.length;
  }

  /**
   * When this is present in the first 8 bytes of a record value this indicates a record value that suports headers.
   */
  public void writeMagicTo(ByteBuffer bbuf) {
    bbuf.put(headerMagicValue);
  }

  /**
   *
   * @param bbuf this modifies position() if true has been returned
   * @return true if the remaining bytes in the byte buffer are headers message
   */
  public boolean isHeaderMessage(ByteBuffer bbuf) {
    if (bbuf.remaining() < magicSize() + VERSION_SIZE) {
      return false;
    }

    boolean isHeaderMessage = true;
    for (int i = 0; i < headerMagicValue.length; i++) {
      isHeaderMessage = headerMagicValue[i] == bbuf.get() && isHeaderMessage;
    }
    if (!isHeaderMessage) {
      bbuf.position(bbuf.position() - magicSize());
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
    byte foundVersion = src.get();
    if (foundVersion != VERSION_1) {
      throw new IllegalStateException("Invalid header format version " + foundVersion + ".");
    }
    while (src.hasRemaining()) {
      int headerKey = src.getInt();
      HeaderKeySpace.validateHeaderKey(headerKey);
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
   * @param headers This writes the version field if headers is null.
   */
  public static void writeHeader(ByteBuffer dest, Map<Integer, byte[]> headers) {
    dest.put(VERSION_1);
    if (headers == null) {
      return;
    }
    for (Map.Entry<Integer, byte[]> header : headers.entrySet()) {
      HeaderKeySpace.validateHeaderKey(header.getKey());

      dest.putInt(header.getKey());
      dest.putInt(header.getValue().length);
      dest.put(header.getValue());
    }
  }

  /**ß
   * The serialized size of all the headers.
   * @return VERSION_SIZE if headers is null else the number of bytes needed to represent the header key and value, but
   * without the magic number.
   */
  public static int serializedHeaderSize(Map<Integer, byte[]> headers) {
    if (headers == null) {
      return VERSION_SIZE;
    }
    int size = headers.size() * 8 + VERSION_SIZE; // size of all the keys and the value length fields
    for (byte[] headerValue : headers.values()) {
      size += headerValue.length;
    }
    return size;
  }

}
