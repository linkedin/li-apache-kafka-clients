/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.auditing.NoOpAuditor;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumerConfig;
import com.linkedin.kafka.clients.utils.DefaultHeaderSerializer;
import com.linkedin.kafka.clients.utils.UUIDFactory;
import com.linkedin.kafka.clients.utils.UUIDFactoryImpl;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * The configuration class for LiKafkaProducer
 */
public class LiKafkaProducerConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String LARGE_MESSAGE_ENABLED_CONFIG = "large.message.enabled";
  public static final String MAX_MESSAGE_SEGMENT_BYTES_CONFIG = "max.message.segment.bytes";
  public static final String AUDITOR_CLASS_CONFIG = "auditor.class";
  public static final String KEY_SERIALIZER_CLASS_CONFIG = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
  public static final String VALUE_SERIALIZER_CLASS_CONFIG = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
  public static final String CURRENT_PRODUCER = "current.producer";
  public static final String UUID_FACTORY_CLASS_CONFIG = "uuid.factory.class";
  public static final String HEADER_SERIALIZER_CONFIG = "header.serializer.class";

  public static final String HEADER_PARSER_DOC = LiKafkaConsumerConfig.HEADER_PARSER_DOC;

  public static final String LARGE_MESSAGE_ENABLED_DOC = "Configure the producer to support large messages or not. " +
      "If large message is enabled, the producer will split the messages whose size is greater than " +
      "max.message.segment.bytes to multiple smaller messages and send them separately.";

  public static final String MAX_MESSAGE_SEGMENT_BYTES_DOC = "The maximum size of a large message segment. " +
      "This configuration is also used as the threshold of the definition of large messages, i.e. " +
      "the producer will only split the messages whose size is greater the maximum allowed segment bytes. " +
      "This configuration does not have any effect if large message is not enabled.";

  public static final String AUDITOR_CLASS_DOC = "The auditor class to do auditing. If no auditor is configured, a " +
      "default no-op auditor will be used.";

  public static final String KEY_SERIALIZER_CLASS_DOC = "The key serializer class";

  public static final String VALUE_SERIALIZER_CLASS_DOC = "The value serializer class";

  public static final String UUID_FACTORY_CLASS_DOC = "A class that implements " + UUIDFactory.class.getName() + ".";

  public static final String CURRENT_PRODUCER_DOC = "A Producer<byte[], byte[]> that can be used to send events to the " +
      "same Kafka cluster as the LiKafkaProducer is sending records to. Technically speaking this is not a " +
      "configuration. We add this configuration because in many cases the auditor needs a producer to send " +
      "messages, it is more efficient to reuse the same producer used by LiKafkaProducer.";

  static {
    CONFIG = new ConfigDef()
        .define(LARGE_MESSAGE_ENABLED_CONFIG, Type.BOOLEAN, "false", Importance.MEDIUM, LARGE_MESSAGE_ENABLED_DOC)
        .define(MAX_MESSAGE_SEGMENT_BYTES_CONFIG, Type.INT, "800000", Importance.MEDIUM, MAX_MESSAGE_SEGMENT_BYTES_DOC)
        .define(AUDITOR_CLASS_CONFIG, Type.CLASS, NoOpAuditor.class.getName(), Importance.MEDIUM, AUDITOR_CLASS_DOC)
        .define(KEY_SERIALIZER_CLASS_CONFIG, Type.CLASS, ByteArraySerializer.class.getName(), Importance.MEDIUM, KEY_SERIALIZER_CLASS_DOC)
        .define(VALUE_SERIALIZER_CLASS_CONFIG, Type.CLASS, ByteArraySerializer.class.getName(), Importance.MEDIUM, VALUE_SERIALIZER_CLASS_DOC)
        .define(UUID_FACTORY_CLASS_CONFIG, Type.CLASS, UUIDFactoryImpl.class, Importance.MEDIUM, UUID_FACTORY_CLASS_DOC)
        .define(HEADER_SERIALIZER_CONFIG, Type.CLASS, DefaultHeaderSerializer.class, Importance.LOW, HEADER_PARSER_DOC);
  }

  LiKafkaProducerConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }

  public Map<String, Object> configsWithCurrentProducer(Producer<byte[], byte[]> producer) {
    Map<String, Object> newConfigs = new HashMap<>();
    newConfigs.putAll(this.originals());
    newConfigs.put(CURRENT_PRODUCER, producer);
    return newConfigs;
  }

}
