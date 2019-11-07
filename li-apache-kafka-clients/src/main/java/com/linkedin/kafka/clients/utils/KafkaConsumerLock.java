/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.util.ConcurrentModificationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;


/**
 * a "lock" that implements kafka consumer locking semantics. <br>
 * note that kafka consumer locking semantics are not good for general purpose locking :-)
 */
public class KafkaConsumerLock implements Lock {

  //the thread that currently holds the lock (==is operating on the consumer). null for none
  private AtomicReference<Thread> ownerThread = new AtomicReference<>(null);
  //"depth" (number of times acquired) for current owner thread. this is to provide reenterance support
  private AtomicInteger refCount = new AtomicInteger(0);

  @Override
  public void lock() {
    if (!tryLock()) {
      Thread owner = ownerThread.get(); //may be null if we got unscheduled. this is just best effort for logging.
      throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access. "
          + "competing thread is " + (owner != null ? owner.getName() : "unknown"));
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    throw new UnsupportedOperationException("not implemented (yet?)");
  }

  @Override
  public boolean tryLock() {
    Thread current = Thread.currentThread();
    Thread owner = ownerThread.get();
    if (owner != current && !ownerThread.compareAndSet(null, current)) {
      //we lost
      return false;
    }
    refCount.incrementAndGet();
    return true;
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("not implemented (yet?)");
  }

  @Override
  public void unlock() {
    if (refCount.decrementAndGet() == 0) {
      ownerThread.set(null);
    }
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException("not implemented (yet?)");
  }
}
