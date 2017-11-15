/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.auditing.abstractimpl;

/**
 * The interface for audit stats. This is used to hold the audit information of a tick in {@link AbstractAuditor}.
 *
 * Users may have a different implementation to have different auditing behavior. An example of the implementation can
 * be found in {@link CountingAuditStats}
 *
 * The implementation of this interface needs to be thread safe.
 */
public interface AuditStats {

  /**
   * The method that record the message for audit.
   *
   * @param auditKey The audit key for the record. (e.g. combination of topic, key and audit type).
   * @param messageCount The number of messages to record.
   * @param bytesCount the number of bytes to record.
   *
   * @throws IllegalStateException Thrown if the audit stats is updated after it is closed.
   */
  void update(Object auditKey, long messageCount, long bytesCount) throws IllegalStateException;

  /**
   * Close the audit stats.
   *
   * The implementation needs to ensure that the stats won't be changed after it is closed.
   */
  void close();
}
