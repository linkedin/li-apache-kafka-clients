/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
  public void record(String topic, K key, V value, Long timestamp, Integer sizeInBytes, AuditType auditType) {

  }

  @Override
  public void close(long timeout, TimeUnit unit) {

  }

  @Override
  public void close() {

  }
}
