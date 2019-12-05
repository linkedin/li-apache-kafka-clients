/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.util.Collection;
import java.util.Iterator;

/**
 * quick and simple unmodifiable implementation of a collection on top of a pair of other collections.
 * @param <T> value type
 */
public class CompositeCollection<T> implements Collection<T> {
  private final Collection<T> a;
  private final Collection<T> b;

  public CompositeCollection(Collection<T> a, Collection<T> b) {
    if (a == null || b == null) {
      throw new IllegalArgumentException("arguments must not be null");
    }
    this.a = a;
    this.b = b;
  }

  @Override
  public int size() {
    return a.size() + b.size();
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
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }
}
