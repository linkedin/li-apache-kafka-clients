/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.security;

import java.util.Map;


public class DefaultTopicEncrypterDecrypterManager implements TopicEncrypterDecrypterManager {

  public DefaultTopicEncrypterDecrypterManager() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    //nop
  }

  @Override
  public KafkaMessageEncrypterDecrypter getEncrypterDecrypter(String topic) {
    // this is a simple implementation by creating a new DefaultKafkaMessageEncrypterDecrypter each time.
    // To optimize, cache can be utilized.
    return new DefaultKafkaMessageEncrypterDecrypter();
  }
}
