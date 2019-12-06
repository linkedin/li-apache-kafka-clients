/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * quick and simple unmodifiable implementation of a set on top of a pair of other sets.
 * note that this class is meant for a particular use case (composition of kafka client metrics)
 * and is written to be fast over perfectly correct
 * @param <T> value type
 */
public class CompositeSet<T> implements Set<T> {
  private final Set<T> a;
  private final Set<T> b;

  public CompositeSet(Set<T> a, Set<T> b) {
    if (a == null || b == null) {
      throw new IllegalArgumentException("arguments must not be null");
    }
    this.a = a;
    this.b = b;
  }

  @Override
  public int size() {
    return a.size() + b.size(); //we assume they're foreign, for perf reasons
  }

  @Override
  public boolean isEmpty() {
    return a.isEmpty() && b.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return a.contains(o) || b.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    return new CompositeIterator<>(a.iterator(), b.iterator());
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <U> U[] toArray(U[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(T t) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }
}
