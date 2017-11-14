/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Annotations to specify whether the interface is from open source or LiKafkaClients only.
 */
public class InterfaceOrigin {

  /**
   * Meaning this method is inherited from Apache Kafka
   */
  @Documented
  @Target(ElementType.METHOD)
  public @interface ApacheKafka {

  }

  /**
   * Meaning this method is defined only in LiKafkaClients.
   */
  @Documented
  @Target(ElementType.METHOD)
  public @interface LiKafkaClients {

  }

}
