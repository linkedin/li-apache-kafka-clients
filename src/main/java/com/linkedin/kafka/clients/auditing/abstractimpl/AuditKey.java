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
    if (obj instanceof AuditKey) {
      AuditKey other = (AuditKey) obj;
      return equals(_topic, other.topic()) && equals(_auditType, other.auditType());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h1 = _topic != null ? _topic.hashCode() : 0;
    int h2 = _bucket != null ? _bucket.hashCode() : 0;
    int h3 = _auditType != null ? _auditType.hashCode() : 0;
    return 31 * 31 * h1 + 31 * h2 + h3;
  }

  @Override
  public String toString() {
    return "(" + _topic + ',' + _bucket + ',' + auditType() + ')';
  }

  private static boolean equals(Object o1, Object o2) {
    if (o1 != null) {
      return o1.equals(o2);
    }
    return o2 == null;
  }

}
