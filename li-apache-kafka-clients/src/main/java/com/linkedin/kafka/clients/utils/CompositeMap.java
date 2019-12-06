/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.util.Collection;
import java.util.Map;
import java.util.Set;


/**
 * quick and simple unmodifiable implementation of a map on top of a pair of other maps.
 * note that this class is meant for a particular use case (composition of kafka client metrics)
 * and is written to be fast over perfectly correct
 * @param <K> key type
 * @param <V> value type
 */
public class CompositeMap<K, V> implements Map<K, V> {
  private final Map<K, V> a;
  private final Map<K, V> b;

  public CompositeMap(Map<K, V> a, Map<K, V> b) {
    if (a == null || a.isEmpty() || b == null || b.isEmpty()) {
      throw new IllegalArgumentException("arguments must be non empty");
    }
    this.a = a;
    this.b = b;
  }

  @Override
  public int size() {
    return a.size() + b.size(); //we assume they're foreign
  }

  @Override
  public boolean isEmpty() {
    return a.isEmpty() && b.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return a.containsKey(key) || b.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return a.containsValue(value) || b.containsValue(value);
  }

  @Override
  public V get(Object key) {
    //this assumes no map container a null value (and is faster than a containsKey + get)
    V v = a.get(key);
    if (v !=  null) {
      return v;
    }
    return b.get(key);
  }

  @Override
  public V put(K key, V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet() {
    return new CompositeSet<>(a.keySet(), b.keySet());
  }

  @Override
  public Collection<V> values() {
    return new CompositeCollection<>(a.values(), b.values());
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return new CompositeSet<>(a.entrySet(), b.entrySet());
  }
}
