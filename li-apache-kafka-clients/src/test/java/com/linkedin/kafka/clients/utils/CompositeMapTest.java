/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CompositeMapTest {

  @Test
  public void testIterations() throws Exception {
    Map<String, Integer> a = new HashMap<>();
    a.put("a", 1);
    a.put("b", 2);
    Map<String, Integer> b = new HashMap<>();
    b.put("c", 3);

    CompositeMap<String, Integer> composite = new CompositeMap<>(a, b);

    Set<String> keys = new HashSet<>();
    Set<Integer> values = new HashSet<>();
    List<Map.Entry<String, Integer>> entries = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : composite.entrySet()) {
      entries.add(entry);
      keys.add(entry.getKey());
      values.add(entry.getValue());
    }
    Assert.assertEquals(entries.size(), 3);
    Assert.assertEquals(entries.size(), 3);
    Assert.assertEquals(keys, new HashSet<>(Arrays.asList("a", "b", "c")));
    Assert.assertEquals(values, new HashSet<>(Arrays.asList(3, 2, 1)));

    keys.clear();
    for (String key : composite.keySet()) {
      keys.add(key);
    }
    Assert.assertEquals(keys, new HashSet<>(Arrays.asList("a", "b", "c")));

    values.clear();
    for (Integer value : composite.values()) {
      values.add(value);
    }
    Assert.assertEquals(values, new HashSet<>(Arrays.asList(3, 2, 1)));
  }
}
