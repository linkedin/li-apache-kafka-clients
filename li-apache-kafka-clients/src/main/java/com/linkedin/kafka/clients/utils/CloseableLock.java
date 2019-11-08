/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.util.concurrent.locks.Lock;


/**
 * a handy decorator around java locks that allows them to be used with try-with-resources expressions
 */
public class CloseableLock implements AutoCloseable {
  private final Lock lock;

  public CloseableLock(Lock lock) {
    this.lock = lock;
    this.lock.lock();
  }

  @Override
  public void close() {
    lock.unlock();
  }
}
