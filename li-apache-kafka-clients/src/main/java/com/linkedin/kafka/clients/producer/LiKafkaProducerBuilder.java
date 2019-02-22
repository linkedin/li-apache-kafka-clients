/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import java.util.Collections;
import java.util.Map;


// A class that builds a LiKafkaProducer using the passed-in producer configs. This is supposed to be used by
// the federated producer to create per-cluster producers.
class LiKafkaProducerBuilder<K, V> {
  private Map<String, Object> _configMap;

  public LiKafkaProducerBuilder() {
    this(Collections.emptyMap());
  }

  public LiKafkaProducerBuilder(Map<String, Object> configMap) {
    _configMap = configMap;
  }

  public LiKafkaProducerBuilder setProducerConfig(Map<String, Object> configMap) {
    _configMap = configMap;
    return this;
  }

  public LiKafkaProducer<K, V> build() {
    // Serializers and auditor will be created using associated producer properties.
    return new LiKafkaProducerImpl<K, V>(_configMap);
  }
}
