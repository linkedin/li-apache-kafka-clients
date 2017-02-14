/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.utils;

import com.linkedin.kafka.clients.consumer.LazyHeaderListMap;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This works with {@link DefaultHeaderSerializer}.
 *  <pre>
 *  8 bytes magic number
 *  1 byte version/flags
 *  4 byte length of all headers
 *  Varies Header key and length fields are encoded in a 4TLV format using network byte ordering.
 *  </pre>
 */
public class DefaultHeaderDeserializer implements HeaderDeserializer, DefaultHeaderSerde {

  /**
   * This does nothing.
   */
  @Override
  public void configure(Map<String, ?> config) {
  }

  @Override
  public DeserializeResult deserializeHeader(ByteBuffer src) {
    if (!isHeaderMessage(src)) {
      return new DeserializeResult(null, src);
    }
    byte versionAndFlags = src.get();
    int foundVersion = versionAndFlags & 0x0F;
    if (foundVersion != DefaultHeaderSerializer.VERSION_1) {
      throw new IllegalStateException("Invalid header format version " + foundVersion + ".");
    }

    int headerLength = src.getInt();
    int origLimit = src.limit();
    src.limit(src.position() + headerLength);
    ByteBuffer headerBuffer = src.slice();
    src.position(src.limit());
    src.limit(origLimit);
    Map<Integer, byte[]> headers = new LazyHeaderListMap(headerBuffer);
    boolean userValueIsNull = (versionAndFlags & USER_VALUE_IS_NULL_FLAG) != 0;
    return new DeserializeResult(headers, userValueIsNull ? null : src);
  }

  /**
   * Callback from LazyHeaderListMap.
   * @param src position should be the beginning of the 4TLV section, limit() should be the end of that section.
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
   * Checks the magic number and length.
   *
   * @param bbuf this modifies position() if true has been returned
   * @return true if the remaining bytes in the byte buffer are headers message
   */
  public boolean isHeaderMessage(ByteBuffer bbuf) {
    if (bbuf.remaining() < _headerMagic.length + VERSION_AND_FLAGS_SIZE + ALL_HEADER_SIZE_FIELD_SIZE) {
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

  private final byte[] _headerMagic;

  public DefaultHeaderDeserializer() {
    this(DefaultHeaderSerde.defaultHeaderMagicBytes());
  }

  /**
   *
   * @param headerMagic  This gets written before the header bytes.  This may be zero length but may not be null.
   */
  protected DefaultHeaderDeserializer(byte[] headerMagic) {
    if (headerMagic == null) {
      throw new IllegalArgumentException("headerMagic must not be null");
    }
    _headerMagic = headerMagic;
  }
}
