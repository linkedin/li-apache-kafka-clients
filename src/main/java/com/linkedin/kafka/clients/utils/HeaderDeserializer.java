/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.utils;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * Reassembles the header part of the message produced by the corresponding {@link HeaderSerializer}.
 */
@InterfaceStability.Unstable
public interface HeaderDeserializer extends Configurable {
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
  DeserializeResult deserializeHeader(ByteBuffer src);

  class DeserializeResult {
    private Map<String, byte[]> _headers;
    private ByteBuffer _value;

    public DeserializeResult(Map<String, byte[]> headers, ByteBuffer value) {
      this._headers = headers;
      this._value = value;
    }

    /**
     *
     * @return This may return null if the message did not have headers.
     */
    public Map<String, byte[]> headers() {
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
}
