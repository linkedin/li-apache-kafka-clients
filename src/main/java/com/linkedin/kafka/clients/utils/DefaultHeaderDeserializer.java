/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.utils;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.BiConsumer;


/**
 * This works with {@link DefaultHeaderSerializer}.
 *  <pre>
 *  8 bytes magic number
 *  1 byte version/flags
 *  4 byte length of all headers
 *  Repeated: 1 byte key length, utf-8 encoded key bytes, 4 byte value length, value bytes
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
    Map<String, byte[]> headers = new LazyHeaderListMap(headerBuffer);
    boolean userValueIsNull = (versionAndFlags & USER_VALUE_IS_NULL_FLAG) != 0;
    return new DeserializeResult(headers, userValueIsNull ? null : src);
  }

  /**
   * Callback from LazyHeaderListMap.
   * @param src position should be the beginning of the repeated section, limit() should be the end of that section.
   * @param mapBuilder a function that will consume the new headers as they are deserialized.
   */
  static void parseHeader(ByteBuffer src, BiConsumer<String, byte[]> mapBuilder) {
    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    CharBuffer charBuffer = CharBuffer.allocate(HeaderUtils.MAX_KEY_LENGTH);
    int originalLimit = src.limit();
    while (src.hasRemaining()) {
      byte headerKeySize = src.get();
      src.limit(src.position() + headerKeySize);
      String headerKey = decodeHeaderKey(src, decoder, charBuffer);
      HeaderUtils.validateHeaderKey(headerKey);
      src.limit(originalLimit);
      int headerValueLength = src.getInt();
      byte[] headerValue = new byte[headerValueLength];
      src.get(headerValue);
      mapBuilder.accept(headerKey, headerValue);
    }
  }

  private static String decodeHeaderKey(ByteBuffer src, CharsetDecoder decoder, CharBuffer charBuffer) {
    charBuffer.position(0);
    charBuffer.limit(HeaderUtils.MAX_KEY_LENGTH);
    decoder.reset();
    CoderResult coderResult = decoder.decode(src, charBuffer, true);
    throwExceptionOnBadEncoding(coderResult);
    coderResult = decoder.flush(charBuffer);
    throwExceptionOnBadEncoding(coderResult);
    charBuffer.flip();
    return charBuffer.toString();
  }

  private static void throwExceptionOnBadEncoding(CoderResult coderResult) {
    if (coderResult.isUnmappable() || coderResult.isOverflow() || coderResult.isMalformed()) {
      throw new IllegalStateException("Failed to decode header key.");
    }
  }

  /**
   * Checks the magic number and length.
   *
   * @param bbuf this modifies position() if true has been returned
   * @return true if the remaining bytes in the byte buffer are headers message
   */
  private boolean isHeaderMessage(ByteBuffer bbuf) {
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
