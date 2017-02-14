/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.Configurable;


/**
 * Implementations define the wire format for headers.  This exists as an interface to support the use case where users
 * have some existing wire format that needs needs to be dealt with or have some internal requirements on message value
 * format.
 */
public interface HeaderSerializerDeserializer extends Configurable {

  /**
   * The maximum size of the all the serialized headers for a single record.
   */
  static final int MAX_SERIALIZED_HEADER_SIZE = 1_024 * 16;

  static class ParseResult {
    private Map<Integer, byte[]> _headers;
    private ByteBuffer _value;

    public ParseResult(Map<Integer, byte[]> headers, ByteBuffer value) {
      this._headers = headers;
      this._value = value;
    }

    /**
     *
     * @return This may return null if the message did not have headers.
     */
    public Map<Integer, byte[]> headers() {
      return _headers;
    }

    /**
     *
     * @return  This may return null in which case the original user value was also null
     */
    public ByteBuffer value() {
      return _value;
    }
  }
  /**
   *
   * @param src This should be the value part of the underlying producer or consumer message.  After a successful call
   *            position and limit should indicate the part of the message where the user value resides.  When src does
   *            not contain the expected format it's position and limit should be the original position and limit.  On
   *            exception position and limit are undefined.
   * @return non-null.  If this is not a valid message with headers then the return value (rv) rv.headers() should be
   * null.
   *
   */
  ParseResult parseHeader(ByteBuffer src);

  /**
   *
   * @param dest The destination byte buffer where we should write the headers to.  If this method throws an exception
   *             the caller can not assume any particular state of dest.
   * @param headers If this is non-null then serialized header bytes will be written to dest.  If this is null then
   *                an implementation may still write something like a magic number to dest.
   * @param nullValue When true the user value of the message is actually null.  This is here so the header format
   *                    may preserve the null when unpacking the user value.
   */
  void writeHeader(ByteBuffer dest, Map<Integer, byte[]> headers, boolean nullValue);

  /**
   * The serialized size of all the headers.  The producer may optionally not serialize an empty header in order to
   * preserve the ability to send truly null values to the broker.
   *
   * @param headers This may be null
   * @return non-negative, if headers is null this may still return a number greater than zero.
   */
  int serializedHeaderSize(Map<Integer, byte[]> headers);
}