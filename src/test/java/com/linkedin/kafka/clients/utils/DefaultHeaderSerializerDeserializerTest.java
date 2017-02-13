/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class DefaultHeaderSerializerDeserializerTest {
  private static final char UNICODE_REPLACEMENT_CHARACTER = 65533;

  @Test
  public void invalidUtf8MagicTest() throws Exception {
    byte[] headerMagicBytes;
    try (ByteArrayOutputStream bout = new ByteArrayOutputStream(8);
      DataOutputStream dout = new DataOutputStream(bout)) {
      dout.writeLong(DefaultHeaderSerializerDeserializer.DEFAULT_HEADER_MAGIC);
      headerMagicBytes = bout.toByteArray();
    }

    for (int i = 0; i < headerMagicBytes.length; i++) {
      byte[] subMagic = Arrays.copyOfRange(headerMagicBytes, i, headerMagicBytes.length);
      // Data input stream fail
      try {
        try (DataInputStream din = new DataInputStream(new ByteArrayInputStream(subMagic))) {
          din.readUTF();
        }
        assertFalse(true, "Expected exception.  Should not have reached here" + i);
      } catch (EOFException e) {
      }

      StringDeserializer stringDeserializer = new StringDeserializer();
      String deserializedBadString = stringDeserializer.deserialize("jkkdfj", subMagic);
      char[] expectedReplacementCharacters = new char[subMagic.length];
      Arrays.fill(expectedReplacementCharacters, UNICODE_REPLACEMENT_CHARACTER);
      assertEquals(deserializedBadString, new String(expectedReplacementCharacters));
    }
  }

  @Test
  public void headerSerializationTest() {
    DefaultHeaderSerializerDeserializer headerSerde = new DefaultHeaderSerializerDeserializer();
    ByteBuffer bbuf = ByteBuffer.allocate(1024*16);
    Map<Integer, byte[]> headers = new HashMap<>();
    String headerValue = "header-value";
    headers.put(42, headerValue.getBytes());
    assertEquals(headerSerde.serializedHeaderSize(headers), 8 + 1 + 4 + 4 + 4 + 12);
    headerSerde.writeHeader(bbuf, headers, false);
    bbuf.flip();
    assertEquals(bbuf.remaining(), headerSerde.serializedHeaderSize(headers));

    HeaderSerializerDeserializer.ParseResult parseResult = headerSerde.parseHeader(bbuf);
    assertFalse(parseResult.value().hasRemaining());
    assertEquals(parseResult.headers().size(), 1);
    assertEquals(parseResult.headers().get(42), headerValue.getBytes());
  }
}
