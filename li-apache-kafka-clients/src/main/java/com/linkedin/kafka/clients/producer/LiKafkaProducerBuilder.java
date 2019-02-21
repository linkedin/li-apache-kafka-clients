/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import java.util.Map;


public class LiKafkaProducerBuilder<K, V> {
  private Map<String, Object> _configMap;

  public LiKafkaProducerBuilder() {
    this(null);
  }

  public LiKafkaProducerBuilder(Map<String, Object> configMap) {
    _configMap = configMap;
  }

  public LiKafkaProducerBuilder setProducerConfig(Map<String, Object> configMap) {
    _configMap = configMap;
    return this;
  }

  public LiKafkaProducer<K, V> build() {
    if (_configMap == null) {
      throw new IllegalStateException("Producer config must be set");
    }
    // Serializers and auditor will be created using associated producer properties.
    return new LiKafkaProducerImpl<K, V>(_configMap);
  }
}
