/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import java.util.Collections;
import java.util.Map;


class LiKafkaConsumerBuilder<K, V> {
  private Map<String, Object> _configMap;

  public LiKafkaConsumerBuilder() {
    this(Collections.emptyMap());
  }

  public LiKafkaConsumerBuilder(Map<String, Object> configMap) {
    _configMap = configMap;
  }

  public LiKafkaConsumerBuilder setConsumerConfig(Map<String, Object> configMap) {
    _configMap = configMap;
    return this;
  }

  public LiKafkaConsumer<K, V> build() {
    // Serializers and auditor will be created using associated consumer properties.
    return new LiKafkaConsumerImpl<K, V>(_configMap);
  }
}