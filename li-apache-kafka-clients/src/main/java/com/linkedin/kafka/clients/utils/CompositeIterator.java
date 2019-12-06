/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.util.Iterator;

/**
 * quick and simple unmodifiable implementation of an iterator on top of a pair of other iterators.
 * @param <T> value type
 */
public class CompositeIterator<T> implements Iterator<T> {
  private final Iterator<T> a;
  private final Iterator<T> b;

  public CompositeIterator(Iterator<T> a, Iterator<T> b) {
    if (a == null || b == null) {
      throw new IllegalArgumentException("arguments must not be null");
    }
    this.a = a;
    this.b = b;
  }

  @Override
  public boolean hasNext() {
    return a.hasNext() || b.hasNext();
  }

  @Override
  public T next() {
    if (a.hasNext()) {
      return a.next();
    }
    return b.next();
  }
}
