/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
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

    @Override
    public String toString() {
      return name();
    }
  };

  public static final AuditType FAILURE = new AuditType() {
    @Override
    public String name() {
      return "FAILURE";
    }

    @Override
    public String toString() {
      return name();
    }
  };

  public static final AuditType ATTEMPT = new AuditType() {
    @Override
    public String name() {
      return "ATTEMPT";
    }

    @Override
    public String toString() {
      return name();
    }
  };
}