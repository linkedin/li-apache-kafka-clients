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

/**
 * The interface of the audit type.
 *
 * There are three predefined audit type used by LiKafkaProducer and LiKafkaConsumer. User may define custom audit type.
 */
public interface AuditType {

  String name();

  public static final AuditType SUCCESS = new AuditType() {
    @Override
    public String name() {
      return "SUCCESS";
    }
  };

  public static final AuditType FAILURE = new AuditType() {
    @Override
    public String name() {
      return "FAILURE";
    }
  };

  public static final AuditType ATTEMPT = new AuditType() {
    @Override
    public String name() {
      return "ATTEMPT";
    }
  };
}