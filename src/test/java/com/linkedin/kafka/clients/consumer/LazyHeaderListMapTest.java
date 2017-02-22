/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.utils.DefaultHeaderDeserializer;
import com.linkedin.kafka.clients.utils.DefaultHeaderSerializer;
import com.linkedin.kafka.clients.utils.HeaderDeserializer;
import com.linkedin.kafka.clients.utils.LazyHeaderListMap;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class LazyHeaderListMapTest {

  private static final byte[] VALUE_0 = new byte[]{ 1, 2, 3, 4, 11};
  private static final byte[] VALUE_1 = new byte[]{ 5, 6, 7, 8};

  @Test
  public void lazyParseHeaders() {
    Map<String, byte[]> expected = new HashMap<>();
    expected.put("header.0", VALUE_0);
    expected.put("header.1", VALUE_1);
    DefaultHeaderSerializer headerSerializer = new DefaultHeaderSerializer();
    int sizeBytes = headerSerializer.serializedHeaderSize(expected);
    ByteBuffer serializedHeaders = ByteBuffer.allocate(sizeBytes);
    headerSerializer.serializeHeader(serializedHeaders, expected, false);
    serializedHeaders.rewind();

    HeaderDeserializer headerDeserializer = new DefaultHeaderDeserializer();
    HeaderDeserializer.DeserializeResult deserializeResult = headerDeserializer.deserializeHeader(serializedHeaders);
    LazyHeaderListMap deserializedMap = (LazyHeaderListMap) deserializeResult.headers();
    assertEquals(deserializedMap.size(), 2);
    assertEquals(deserializedMap.get("header.0"), VALUE_0);
    assertEquals(deserializedMap.get("header.1"), VALUE_1);

    Iterator<String> keyIterator = deserializedMap.keySet().iterator();
    Set<String> expectedKeys = new HashSet<>();
    expectedKeys.add("header.0");
    expectedKeys.add("header.1");

    Set<String> actualKeys = new HashSet<>();
    actualKeys.add(keyIterator.next());
    actualKeys.add(keyIterator.next());
    assertEquals(actualKeys, expectedKeys);
    assertFalse(keyIterator.hasNext());
  }
}
