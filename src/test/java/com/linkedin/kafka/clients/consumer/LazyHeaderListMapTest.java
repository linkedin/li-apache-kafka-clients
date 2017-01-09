/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.utils.HeaderParser;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.junit.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class LazyHeaderListMapTest {

  private static final byte[] VALUE_0 = new byte[]{ 1, 2, 3, 4, 11};
  private static final byte[] VALUE_1 = new byte[]{ 5, 6, 7, 8};

  @Test
  public void lazyParseHeaders() {
    Map<Integer, byte[]> expected = new HashMap<>();
    expected.put(0, VALUE_0);
    expected.put(1, VALUE_1);
    int sizeBytes = HeaderParser.serializedHeaderSize(expected);
    ByteBuffer serializedHeaders = ByteBuffer.allocate(sizeBytes);
    HeaderParser.writeHeader(serializedHeaders, expected);
    serializedHeaders.rewind();
    LazyHeaderListMap deserializedMap = new LazyHeaderListMap(serializedHeaders);
    Assert.assertEquals(deserializedMap.size(), 2);
    Assert.assertArrayEquals(deserializedMap.get(0), VALUE_0);
    Assert.assertArrayEquals(deserializedMap.get(1), VALUE_1);

    Iterator<Integer> keyIterator = deserializedMap.keySet().iterator();
    assertEquals(keyIterator.next().intValue(), 0);
    assertEquals(keyIterator.next().intValue(), 1);
    assertFalse(keyIterator.hasNext());
  }
}
