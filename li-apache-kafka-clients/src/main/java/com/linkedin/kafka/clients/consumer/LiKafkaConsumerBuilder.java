/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import java.util.Map;


public class LiKafkaConsumerBuilder<K, V> {
  private Map<String, Object> _configMap;

  public LiKafkaConsumerBuilder() {
    this(null);
  }

  public LiKafkaConsumerBuilder(Map<String, Object> configMap) {
    _configMap = configMap;
  }

  public LiKafkaConsumerBuilder setConsumerConfig(Map<String, Object> configMap) {
    _configMap = configMap;
    return this;
  }

  public LiKafkaConsumer<K, V> build() {
    if (_configMap == null) {
      throw new IllegalStateException("Consumer config must be set");
    }
    // Serializers and auditor will be created using associated Consumer properties.
    return new LiKafkaConsumerImpl<K, V>(_configMap);
  }
}