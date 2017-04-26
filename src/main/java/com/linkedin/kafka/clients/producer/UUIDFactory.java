package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * The UUID factory class to generate UUID.
 */
public interface UUIDFactory<K, V> {

  /**
   * Get a random UUID.
   */
  UUID createUuid();

  /**
   * Get the UUID based on the producer record.
   */
  UUID getUuid(ProducerRecord<K, V> record);

  /**
   * The default implementation of UUIDFactory.
   */
  class DefaultUUIDFactory<K, V> implements UUIDFactory<K, V> {

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
