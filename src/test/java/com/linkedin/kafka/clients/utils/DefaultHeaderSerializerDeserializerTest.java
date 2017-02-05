/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.util.Arrays;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DefaultHeaderSerializerDeserializerTest {
  private static final char UNICODE_REPLACEMENT_CHARACTER = 65533;

  @Test
  public void invalidUtf8Magic() throws Exception {
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
        Assert.assertFalse(true, "Expected exception.  Should not have reached here" + i);
      } catch (EOFException e) {
      }

      StringDeserializer stringDeserializer = new StringDeserializer();
      String deserializedBadString = stringDeserializer.deserialize("jkkdfj", subMagic);
      char[] expectedReplacementCharacters = new char[subMagic.length];
      Arrays.fill(expectedReplacementCharacters, UNICODE_REPLACEMENT_CHARACTER);
      Assert.assertEquals(deserializedBadString, new String(expectedReplacementCharacters));
    }


  }
}
