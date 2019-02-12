/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;


// A builder class that builds per-cluster raw byte producers.
public class RawByteProducerBuilder {
  private LiKafkaProducerConfig _producerConfig;

  public RawByteProducerBuilder() {
    this(null);
  }

  public RawByteProducerBuilder(LiKafkaProducerConfig producerConfig) {
    _producerConfig = producerConfig;
  }

  public RawByteProducerBuilder setProducerConfig(LiKafkaProducerConfig producerConfig) {
    _producerConfig = producerConfig;
    return this;
  }

  public Producer<byte[], byte[]> build() {
    return new KafkaProducer<>(_producerConfig.originals(), new ByteArraySerializer(), new ByteArraySerializer());
  }
}
