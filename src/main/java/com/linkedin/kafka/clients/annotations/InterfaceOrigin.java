/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
