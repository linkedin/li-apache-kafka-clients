/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.auditing.NoOpAuditor;
import com.linkedin.kafka.clients.common.LiKafkaCommonClientConfigs;
import com.linkedin.kafka.clients.largemessage.DefaultSegmentSerializer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * The configuration class for LiKafkaProducer
 */
public class LiKafkaProducerConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String LARGE_MESSAGE_ENABLED_CONFIG = "large.message.enabled";
  public static final String HEADER_TIMESTAMP_ENABLED = "header.timestamp.enabled";
  public static final String MAX_MESSAGE_SEGMENT_BYTES_CONFIG = "max.message.segment.bytes";
  public static final String AUDITOR_CLASS_CONFIG = "auditor.class";
  public static final String KEY_SERIALIZER_CLASS_CONFIG = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
  public static final String VALUE_SERIALIZER_CLASS_CONFIG = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
  public static final String PARTITIONER_CLASS_CONFIG = ProducerConfig.PARTITIONER_CLASS_CONFIG;
  public static final String MAX_BLOCK_MS_CONFIG = ProducerConfig.MAX_BLOCK_MS_CONFIG;
  public static final String SEGMENT_SERIALIZER_CLASS_CONFIG = "segment.serializer";
  public static final String UUID_FACTORY_CLASS_CONFIG = "uuid.factory.class";
  public static final String CURRENT_PRODUCER = "current.producer";
  public static final String METADATA_SERVICE_CLIENT_CLASS_CONFIG =
      LiKafkaCommonClientConfigs.METADATA_SERVICE_CLIENT_CLASS_CONFIG;
  public static final String METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG =
      LiKafkaCommonClientConfigs.METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG;
  public static final String CLUSTER_GROUP_CONFIG = LiKafkaCommonClientConfigs.CLUSTER_GROUP_CONFIG;
  public static final String CLUSTER_ENVIRONMENT_CONFIG = LiKafkaCommonClientConfigs.CLUSTER_ENVIRONMENT_CONFIG;
  public static final String MAX_REQUEST_SIZE_CONFIG = ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
  public static final String LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_CONFIG =
      "li.large.message.segment.wrapping.required";
  public static final String ENABLE_RECORD_HEADER_CONFIG = "li.record.header.enable";

  public static final String LARGE_MESSAGE_ENABLED_DOC = "Configure the producer to support large messages or not. " +
      "If large message is enabled, the producer will split the messages whose size is greater than " +
      "max.message.segment.bytes to multiple smaller messages and send them separately.";

  public static final String HEADER_TIMESTAMP_ENABLED_DOC = "Configure the producer to stamp event time in the headers. "
      + "Note that, any previously stamped values will be overwritten. "
      + "If enabled, the new timestamp in the header will be equal to ProducerRecord.timestamp() if non-null "
      + "otherwise, System.currentTimeInMillis() will be used.";
  public static final String MAX_MESSAGE_SEGMENT_BYTES_DOC = "The maximum size of a large message segment. " +
      "This configuration is also used as the threshold of the definition of large messages, i.e. " +
      "the producer will only split the messages whose size is greater the maximum allowed segment bytes. " +
      "This configuration does not have any effect if large message is not enabled.";

  public static final String AUDITOR_CLASS_DOC = "The auditor class to do auditing. If no auditor is configured, a " +
      "default no-op auditor will be used.";

  public static final String KEY_SERIALIZER_CLASS_DOC = "The key serializer class";

  public static final String VALUE_SERIALIZER_CLASS_DOC = "The value serializer class";

  public static final String PARTITIONER_CLASS_CONFIG_DOC = "The partitioner class";

  public static final String MAX_BLOCK_MS_DOC = "see max.block.ms in kafka producer documentation";

  public static final String SEGMENT_SERIALIZER_CLASS_DOC = "The class of segment serializer. The segment serializer " +
      "will be used to serialize large message segments when large message is enabled for LiKafkaProducer.";

  public static final String UUID_FACTORY_CLASS_DOC = "The UUID factory class to use for UUID generation.";

  public static final String METADATA_SERVICE_CLIENT_CLASS_DOC =
      LiKafkaCommonClientConfigs.METADATA_SERVICE_CLIENT_CLASS_DOC;

  public static final String METADATA_SERVICE_REQUEST_TIMEOUT_MS_DOC =
      LiKafkaCommonClientConfigs.METADATA_SERVICE_REQUEST_TIMEOUT_MS_DOC;

  public static final String CLUSTER_GROUP_DOC = LiKafkaCommonClientConfigs.CLUSTER_GROUP_DOC;

  public static final String CLUSTER_ENVIRONMENT_DOC = LiKafkaCommonClientConfigs.CLUSTER_ENVIRONMENT_DOC;

  public static final String MAX_REQUEST_SIZE_DOC = "Maximum request size";

  public static final String LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_DOC = "Whether or not to wrap every message " +
      "payload in a large message segment, even if the original payload is smaller than max.message.segment.bytes " +
      "and thus not split into multiple messages. This configuration does not have any effect if large message is " +
      "not enabled.";

  public static final String ENABLE_RECORD_HEADER_DOC = "If true, we will utilize record headers for large message support and other serialization(e.g. encryption or compression) and stop using segmentSerializer.";

  static {
    // TODO: Add a default metadata service client class.
    CONFIG = new ConfigDef()
        .define(LARGE_MESSAGE_ENABLED_CONFIG, Type.BOOLEAN, "false", Importance.MEDIUM, LARGE_MESSAGE_ENABLED_DOC)
        .define(HEADER_TIMESTAMP_ENABLED, Type.BOOLEAN, "true", Importance.MEDIUM, HEADER_TIMESTAMP_ENABLED_DOC)
        .define(MAX_MESSAGE_SEGMENT_BYTES_CONFIG, Type.INT, "800000", Importance.MEDIUM, MAX_MESSAGE_SEGMENT_BYTES_DOC)
        .define(AUDITOR_CLASS_CONFIG, Type.CLASS, NoOpAuditor.class.getName(), Importance.MEDIUM, AUDITOR_CLASS_DOC)
        .define(KEY_SERIALIZER_CLASS_CONFIG, Type.CLASS, ByteArraySerializer.class.getName(), Importance.MEDIUM, KEY_SERIALIZER_CLASS_DOC)
        .define(VALUE_SERIALIZER_CLASS_CONFIG, Type.CLASS, ByteArraySerializer.class.getName(), Importance.MEDIUM, VALUE_SERIALIZER_CLASS_DOC)
        //we set null as the default partitioner because the "vanilla" kafka default partitioner is "safe" for large message
        //support and we'd rather not pay the cost of making reflection calls for metadata in send() if we dont have to.
        .define(PARTITIONER_CLASS_CONFIG, Type.CLASS, null, Importance.MEDIUM, PARTITIONER_CLASS_CONFIG_DOC)
        .define(MAX_BLOCK_MS_CONFIG, Type.LONG, 60 * 1000, atLeast(0), Importance.MEDIUM, MAX_BLOCK_MS_DOC)
        .define(SEGMENT_SERIALIZER_CLASS_CONFIG, Type.CLASS, DefaultSegmentSerializer.class.getName(), Importance.MEDIUM, SEGMENT_SERIALIZER_CLASS_DOC)
        .define(UUID_FACTORY_CLASS_CONFIG, Type.CLASS, UUIDFactory.DefaultUUIDFactory.class.getName(), Importance.LOW, UUID_FACTORY_CLASS_DOC)
        .define(METADATA_SERVICE_CLIENT_CLASS_CONFIG, Type.CLASS, null, Importance.MEDIUM, METADATA_SERVICE_CLIENT_CLASS_DOC)
        .define(CLUSTER_GROUP_CONFIG, Type.STRING, "", Importance.MEDIUM, CLUSTER_GROUP_DOC)
        .define(CLUSTER_ENVIRONMENT_CONFIG, Type.STRING, "", Importance.MEDIUM, CLUSTER_ENVIRONMENT_DOC)
        .define(MAX_REQUEST_SIZE_CONFIG, Type.INT, 1 * 1024 * 1024, atLeast(0), Importance.MEDIUM, MAX_REQUEST_SIZE_DOC)
        .define(LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_CONFIG, Type.BOOLEAN, "false", Importance.MEDIUM,
            LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_DOC)
        .define(METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG, Type.INT, Integer.MAX_VALUE, Importance.MEDIUM,
            METADATA_SERVICE_REQUEST_TIMEOUT_MS_DOC)
        .define(ENABLE_RECORD_HEADER_CONFIG, Type.BOOLEAN, "false", Importance.MEDIUM, ENABLE_RECORD_HEADER_DOC);
    ;
  }

  LiKafkaProducerConfig(Map<?, ?> props) {
    super(CONFIG, props, false);
  }

  public <T> T getConfiguredInstance(String key, Class<T> t, Producer<byte[], byte[]> producer) {
    Class<?> c = getClass(key);
    if (c == null) {
      return null;
    }
    Object o = Utils.newInstance(c);

    if (!t.isInstance(o)) {
      throw new KafkaException(c.getName() + " is not an instance of " + t.getName());
    }

    if (o instanceof Configurable) {
      ((Configurable) o).configure(configsWithCurrentProducer(producer));
    }

    return t.cast(o);
  }

  Map<String, Object> configsWithCurrentProducer(Producer<byte[], byte[]> producer) {
    Map<String, Object> newConfigs = new HashMap<>();
    newConfigs.putAll(this.originals());
    newConfigs.put(CURRENT_PRODUCER, producer);
    return newConfigs;
  }
}
