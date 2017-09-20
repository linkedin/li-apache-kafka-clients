/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;


/**
 * The UUID factory class to generate UUID.
 */
public interface UUIDFactory<K, V> extends Configurable {

  /**
   * @return a random UUID
   */
  UUID createUuid();

  /**
   * @param record a producer record to get the UUID from/by
   * @return a UUID based on the producer record.
   */
  UUID getUuid(ProducerRecord<K, V> record);

  /**
   * The default implementation of UUIDFactory.
   */
  class DefaultUUIDFactory<K, V> implements UUIDFactory<K, V> {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public UUID createUuid() {
      return LiKafkaClientsUtils.randomUUID();
    }

    @Override
    public UUID getUuid(ProducerRecord<K, V> record) {
      return LiKafkaClientsUtils.randomUUID();
    }
  }
}
