/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.auditing;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The default no-op auditor class.
 */
public class NoOpAuditor<K, V> implements Auditor<K, V> {

  @Override
  public void configure(Map<String, ?> configs) {

  }

  @Override
  public void start() {

  }

  @Override
  public Object auditToken(K key, V value) {
    return null;
  }

  @Override
  public void record(Object auditToken,
                     String topic,
                     Long timestamp,
                     Long messageCount,
                     Long bytesCount,
                     AuditType auditType) {

  }


  @Override
  public void close(long timeout, TimeUnit unit) {

  }

  @Override
  public void close() {

  }
}
