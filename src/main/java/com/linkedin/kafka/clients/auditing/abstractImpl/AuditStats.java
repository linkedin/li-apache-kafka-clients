/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.auditing.abstractImpl;

/**
 * The interface for audit stats. This is used to hold the audit information of a tick in {@link AbstractAuditor}.
 *
 * Users may have a different implementation to have different auditing behavior. An example of the implementation can
 * be found in {@link CountingAuditStats}
 *
 * The implementation of this interface needs to be thread safe.
 */
public interface AuditStats<K, V> {

  /**
   * The method that record the message for audit.
   *
   * @param auditKey The audit key for the event. (e.g. combination of topic, key and audit type).
   * @param sizeInBytes the size of the message after serialization.
   *
   * @throws IllegalStateException Thrown if the audit stats is updated after it is closed.
   */
  void update(Object auditKey, int sizeInBytes) throws IllegalStateException;

  /**
   * Close the audit stats.
   *
   * The implementation needs to ensure that the stats won't be changed after it is closed.
   */
  void close();
}
