/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.auditing.abstractimpl;

import com.linkedin.kafka.clients.auditing.AuditType;

import java.util.Objects;

/**
 * This class is an example of AuditKey implementation.
 * The AuditKey we defined here is simply a combination of the topic, bucket and audit type. For different use
 * cases, user may want to define a different audit key.
 */
public final class AuditKey {
  private final String _topic;
  private final Long _bucket;
  private final AuditType _auditType;

  public AuditKey(String topic, Long bucket, AuditType auditType) {
    _topic = topic;
    _bucket = bucket;
    _auditType = auditType;
  }

  public String topic() {
    return _topic;
  }

  public Long bucket() {
    return _bucket;
  }

  public AuditType auditType() {
    return _auditType;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    AuditKey auditKey = (AuditKey) obj;
    return Objects.equals(_topic, auditKey.topic()) && Objects.equals(_bucket, auditKey.bucket())
        && Objects.equals(_auditType, auditKey.auditType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(_topic, _bucket, _auditType);
  }

  @Override
  public String toString() {
    return "(" + _topic + ',' + _bucket + ',' + auditType() + ')';
  }

}
