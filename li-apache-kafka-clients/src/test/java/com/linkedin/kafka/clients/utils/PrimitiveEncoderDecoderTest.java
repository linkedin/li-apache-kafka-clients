/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class PrimitiveEncoderDecoderTest {
  private void verifyLong(long x) {
    byte[] data = new byte[8];
    PrimitiveEncoderDecoder.encodeLong(x, data, 0);
    Assert.assertEquals(data.length, 8);
    Assert.assertEquals(PrimitiveEncoderDecoder.decodeLong(data, 0), x);
    Assert.assertEquals(PrimitiveEncoderDecoder.decodeLong(PrimitiveEncoderDecoder.encodeLong(x), 0), x);
  }

  private void verifyInt(int x) {
    byte[] data = new byte[4];
    PrimitiveEncoderDecoder.encodeInt(x, data, 0);
    Assert.assertEquals(data.length, 4);
    Assert.assertEquals(PrimitiveEncoderDecoder.decodeInt(data, 0), x);
    Assert.assertEquals(PrimitiveEncoderDecoder.decodeInt(PrimitiveEncoderDecoder.encodeInt(x), 0), x);
  }

  @Test
  public void testEncodeDecodeLong() {
    verifyLong(Long.MIN_VALUE);
    verifyLong(Long.MAX_VALUE);
    verifyLong(-1L);
    verifyLong(0L);
    verifyLong(1L);
    verifyLong(1000000000L);
    verifyLong(-1000000000L);
    verifyLong(System.currentTimeMillis());

    verifyInt(Integer.MIN_VALUE);
    verifyInt(Integer.MAX_VALUE);
    verifyInt(-1);
    verifyInt(0);
    verifyInt(1);
    verifyInt(1000000000);
    verifyInt(-1000000000);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testInsufficientDataForDecodeLong() {
    long value = 100;
    byte[] serialized = PrimitiveEncoderDecoder.encodeLong(value);
    Assert.assertNotNull(serialized);
    Assert.assertEquals(serialized.length, 8);
    Assert.assertNotEquals(PrimitiveEncoderDecoder.decodeLong(serialized, 1), value);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testInsufficientDataForDecodeInt() {
    int value = 100;
    byte[] serialized = PrimitiveEncoderDecoder.encodeInt(value);
    Assert.assertNotNull(serialized);
    Assert.assertEquals(serialized.length, 4);
    Assert.assertNotEquals(PrimitiveEncoderDecoder.decodeLong(serialized, 1), value);
  }
}
