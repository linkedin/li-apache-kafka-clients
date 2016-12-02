/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
