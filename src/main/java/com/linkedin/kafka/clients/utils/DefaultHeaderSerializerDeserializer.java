/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.utils;
import com.linkedin.kafka.clients.consumer.LazyHeaderListMap;
import java.nio.ByteBuffer;
import java.util.Map;


/**
 *  <pre>
 *  8 bytes magic number
 *  1 byte version/flags
 *  4 byte length of all headers
 *  Varies Header key and length fields are encoded in a 4TLV format using network byte ordering.
 *  </pre>
 */
public class DefaultHeaderSerializerDeserializer implements HeaderSerializerDeserializer {

  /**
   *   All this bit manipulation makes this number look like an invalid UTF8 encoded string if someone starts to read it
   * within the first 7 bytes
   */
  public static final long DEFAULT_HEADER_MAGIC
    = (0x4c6d4eef4b7a44L | 0b11000000_11000000_11000000_11000000_11000000_11000000_11000000_11000000L) &
    0b11011111_11011111_11011111_11011111_11011111_11011111_11011111_11011111L;

  private static final byte VERSION_1 = 1;

  private static final int VERSION_SIZE = 1;

  private static final int ALL_HEADER_SIZE_FIELD_SIZE = 4;

  private static final byte[] DEFAULT_HEADER_MAGIC_AS_BYTES;

  private static final byte USER_VALUE_IS_NULL_FLAG = 0x10;


  @Override
  public ParseResult parseHeader(ByteBuffer src) {
    if (!isHeaderMessage(src)) {
      return null;
    }
    byte versionAndFlags = src.get();
    int foundVersion = versionAndFlags & 0x0F;
    if (foundVersion != DefaultHeaderSerializerDeserializer.VERSION_1) {
      throw new IllegalStateException("Invalid header format version " + foundVersion + ".");
    }

    int headerLength = src.getInt();
    int origLimit = src.limit();
    src.limit(src.position() + headerLength);
    ByteBuffer headerBuffer = src.slice();
    src.position(src.limit());
    src.limit(origLimit);
    Map<Integer, byte[]> headers = new LazyHeaderListMap(headerBuffer);
    boolean userValueIsNull = (versionAndFlags & USER_VALUE_IS_NULL_FLAG) == 1;
    return new ParseResult(headers, userValueIsNull);
  }

  /**
   * Callback from LazyHeaderListMap.
   */
  public static void parseHeader(ByteBuffer src, Map<Integer, byte[]> headerMap) {
    while (src.hasRemaining()) {
      int headerKey = src.getInt();
      HeaderKeySpace.validateHeaderKey(headerKey);
      int headerValueLength = src.getInt();
      byte[] headerValue = new byte[headerValueLength];
      src.get(headerValue);
      headerMap.put(headerKey, headerValue);
    }
  }

  /**
   * @param dest The destination byte buffer where we should write the headers to.  If this method throws an exception
   *             the caller can not assume any particular state of dest.
   * @param headers This writes the version field if headers is null.
   */
  @Override
  public void writeHeader(ByteBuffer dest, Map<Integer, byte[]> headers, boolean userValueIsNull) {
    int originalPosition = dest.position();
    dest.put(_headerMagic);
    byte versionAndFlags = (byte) (VERSION_1 | (userValueIsNull ? USER_VALUE_IS_NULL_FLAG : 0));
    dest.put(versionAndFlags);
    dest.putInt(0); //updated later
    if (headers == null) {
      return;
    }

    int size = headers.size() * (4 /* header value size */ + 4 /* key */);
    for (Map.Entry<Integer, byte[]> header : headers.entrySet()) {
      HeaderKeySpace.validateHeaderKey(header.getKey());
      dest.putInt(header.getKey());
      dest.putInt(header.getValue().length);
      dest.put(header.getValue());
      size += header.getValue().length;
    }

    dest.putInt(size, originalPosition + _headerMagic.length + 1 /* version and flags*/);
  }

  /**
   * The serialized size of all the headers.
   * @return VERSION_SIZE if headers is null else the number of bytes needed to represent the header key and value, but
   * without the magic number.
   */
  @Override
  public int serializedHeaderSize(Map<Integer, byte[]> headers) {
    if (headers == null) {
      return DefaultHeaderSerializerDeserializer.VERSION_SIZE;
    }
    int size = headers.size() * 8 + VERSION_SIZE + ALL_HEADER_SIZE_FIELD_SIZE + _headerMagic.length; // size of all the keys and the value length fields
    for (byte[] headerValue : headers.values()) {
      size += headerValue.length;
    }
    return size;
  }

  /** This does nothing */
  @Override
  public void configure(Map<String, ?> configs) {

  }

  static {
    ByteBuffer bbuf = ByteBuffer.allocate(8);
    bbuf.putLong(DEFAULT_HEADER_MAGIC);
    DEFAULT_HEADER_MAGIC_AS_BYTES = bbuf.array();
  }


  private final byte[] _headerMagic;

  public DefaultHeaderSerializerDeserializer() {
    this(DEFAULT_HEADER_MAGIC_AS_BYTES);
  }

  /**
   *
   * @param headerMagic  This gets written before the header bytes.  This may be zero length but may not be null.
   */
  protected DefaultHeaderSerializerDeserializer(byte[] headerMagic) {
    if (headerMagic == null) {
      throw new IllegalArgumentException("headerMagic must not be null");
    }
    _headerMagic = headerMagic;
  }

  /**
   * Checks the magic number and length.
   *
   * @param bbuf this modifies position() if true has been returned
   * @return true if the remaining bytes in the byte buffer are headers message
   */
  public boolean isHeaderMessage(ByteBuffer bbuf) {
    if (bbuf.remaining() < _headerMagic.length + VERSION_SIZE + ALL_HEADER_SIZE_FIELD_SIZE) {
      return false;
    }

    boolean isHeaderMessage = true;
    for (int i = 0; i < _headerMagic.length; i++) {
      isHeaderMessage = _headerMagic[i] == bbuf.get() && isHeaderMessage;
    }
    if (!isHeaderMessage) {
      bbuf.position(bbuf.position() - _headerMagic.length);
    }
    return isHeaderMessage;
  }
}
