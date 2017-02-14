/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.utils;
import java.nio.ByteBuffer;
import java.util.Map;


/**
 * This works with {@link DefaultHeaderDeserializer}.
 *  <pre>
 *  8 bytes magic number
 *  1 byte version/flags
 *  4 byte length of all headers
 *  Varies Header key and length fields are encoded in a 4TLV format using network byte ordering.
 *  </pre>
 */
public class DefaultHeaderSerializer implements HeaderSerializer, DefaultHeaderSerde {


  /**
   * @param dest The destination byte buffer where we should write the headers to.  If this method throws an exception
   *             the caller can not assume any particular state of dest.
   * @param headers This writes the version field if headers is null.
   */
  @Override
  public void serializeHeader(ByteBuffer dest, Map<Integer, byte[]> headers, boolean userValueIsNull) {
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

    dest.putInt(originalPosition + _headerMagic.length + VERSION_AND_FLAGS_SIZE, size);
  }

  /**
   * The serialized size of all the headers.
   * @return VERSION_AND_FLAGS_SIZE if headers is null else the number of bytes needed to represent the header key and value, but
   * without the magic number.
   */
  @Override
  public int serializedHeaderSize(Map<Integer, byte[]> headers) {
    if (headers == null) {
      return DefaultHeaderSerializer.VERSION_AND_FLAGS_SIZE;
    }
    // size of all the keys and the value length fields
    int size = headers.size() * 8 + VERSION_AND_FLAGS_SIZE + ALL_HEADER_SIZE_FIELD_SIZE + _headerMagic.length;
    for (byte[] headerValue : headers.values()) {
      size += headerValue.length;
    }
    return size;
  }

  /** This does nothing */
  @Override
  public void configure(Map<String, ?> configs) {

  }

  private final byte[] _headerMagic;

  public DefaultHeaderSerializer() {
    this(DefaultHeaderSerde.defaultHeaderMagicBytes());
  }

  /**
   *
   * @param headerMagic  This gets written before the header bytes.  This may be zero length but may not be null.
   */
  protected DefaultHeaderSerializer(byte[] headerMagic) {
    if (headerMagic == null) {
      throw new IllegalArgumentException("headerMagic must not be null");
    }
    _headerMagic = headerMagic;
  }

}
