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
import static com.linkedin.kafka.clients.utils.DefaultHeaderSerde.utf8StringLength;

public class DefaultHeaderSerializerTest {
  private static final char UNICODE_REPLACEMENT_CHARACTER = 65533;

  @Test
  public void invalidUtf8MagicTest() throws Exception {
    byte[] headerMagicBytes;
    try (ByteArrayOutputStream bout = new ByteArrayOutputStream(8);
      DataOutputStream dout = new DataOutputStream(bout)) {
      dout.writeLong(DefaultHeaderSerializer.DEFAULT_HEADER_MAGIC);
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
    DefaultHeaderSerializer headerSerializer = new DefaultHeaderSerializer();

    Map<String, byte[]> headers = new HashMap<>();
    String headerValue = "header-value";
    String headerKey = "header-key";
    headers.put(headerKey, headerValue.getBytes());
    assertEquals(headerSerializer.serializedHeaderSize(headers),
        8 /* magic */ + 1 /* ver */ + 4 /* totallen */ + 1 /* header-key len */ + headerKey.length() +  4 /* value len */ +
            headerValue.length());
    ByteBuffer bbuf = ByteBuffer.wrap(headerSerializer.serializeHeaderWithValue(headers, ByteBuffer.allocate(0)));
    assertEquals(bbuf.remaining(), headerSerializer.serializedHeaderSize(headers));

    HeaderDeserializer headerDeserializer = new DefaultHeaderDeserializer();
    HeaderDeserializer.DeserializeResult deserializeResult = headerDeserializer.deserializeHeader(bbuf);
    assertFalse(deserializeResult.value().hasRemaining());
    assertEquals(deserializeResult.headers().size(), 1);
    assertEquals(deserializeResult.headers().get(headerKey), headerValue.getBytes());
  }

  @Test
  public void utf8LengthTest() {
    assertEquals(utf8StringLength(""), 0);
    assertEquals(utf8StringLength("\0"), 1);
    assertEquals(utf8StringLength("a"), 1);
    assertEquals(utf8StringLength("\u007f"), 1);
    assertEquals(utf8StringLength("\u0080"), 2);
    assertEquals(utf8StringLength("\u07FF"), 2);
    assertEquals(utf8StringLength("\u0800"), 3);
    assertEquals(utf8StringLength("\uFFFF"), 3);
    String startSupplemental = new String(new int[] { 0x10000}, 0, 1);
    String endSupplemental = new String(new int[] { 0x10FFFF}, 0, 1);
    assertEquals(utf8StringLength(startSupplemental), 4);
    assertEquals(utf8StringLength(endSupplemental), 4);
    String doughnut = new String(new int[] { 0x1F369}, 0, 1);
    assertEquals(utf8StringLength(doughnut), 4);
  }
}
