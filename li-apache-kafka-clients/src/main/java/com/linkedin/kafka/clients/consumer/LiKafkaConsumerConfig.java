/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.auditing.NoOpAuditor;
import com.linkedin.kafka.clients.common.LiKafkaCommonClientConfigs;
import com.linkedin.kafka.clients.largemessage.DefaultSegmentDeserializer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * The configuration class for LiKafkaConsumer
 */
public class LiKafkaConsumerConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String MESSAGE_ASSEMBLER_BUFFER_CAPACITY_CONFIG = "message.assembler.buffer.capacity";
  public static final String MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_CONFIG = "message.assembler.expiration.offset.gap";
  public static final String MAX_TRACKED_MESSAGES_PER_PARTITION_CONFIG = "max.tracked.messages.per.partition";
  public static final String EXCEPTION_ON_MESSAGE_DROPPED_CONFIG = "exception.on.message.dropped";
  public static final String KEY_DESERIALIZER_CLASS_CONFIG = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
  public static final String VALUE_DESERIALIZER_CLASS_CONFIG = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
  public static final String SEGMENT_DESERIALIZER_CLASS_CONFIG = "segment.deserializer.class";
  public static final String AUDITOR_CLASS_CONFIG = "auditor.class";
  public static final String ENABLE_AUTO_COMMIT_CONFIG = ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
  public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
  public static final String AUTO_OFFSET_RESET_CONFIG = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
  public static final String MAX_POLL_RECORDS_CONFIG = ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
  public static final String METADATA_SERVICE_CLIENT_CLASS_CONFIG =
      LiKafkaCommonClientConfigs.METADATA_SERVICE_CLIENT_CLASS_CONFIG;
  public static final String METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG =
      LiKafkaCommonClientConfigs.METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG;
  public static final String CLUSTER_GROUP_CONFIG = LiKafkaCommonClientConfigs.CLUSTER_GROUP_CONFIG;
  public static final String CLUSTER_ENVIRONMENT_CONFIG = LiKafkaCommonClientConfigs.CLUSTER_ENVIRONMENT_CONFIG;

  private static final String MESSAGE_ASSEMBLER_BUFFER_CAPACITY_DOC = "The maximum number of bytes the message assembler " +
      " uses to buffer the incomplete large message segments. The capacity is shared by messages from all the topics. " +
      "If the capacity of the message assembler has been reached, the consumer will drop the oldest incomplete message " +
      "in the buffer.";

  private static final String MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_DOC = "The message assembler will expire and " +
      "discard the buffered large message segments of an incomplete large message if that message has sit in the buffer " +
      "for too long. The expiration is based on the difference between current consumer offset of the partition and the " +
      "starting offset of the incomplete large message. When the current consumer offset of the partition is greater " +
      "than the starting offset of the incomplete large message + the value of this configuration, the incomplete large " +
      "message will be removed. The consumer may throw an exception depending on whether the user has set " +
      "exception.on.message.dropped to true or false.";

  private static final String MAX_TRACKED_MESSAGES_PER_PARTITION_DOC = "In order to support large messages, LiKafkaConsumer " +
      "keeps track of the messages that are delivered. This configuration sets the maximum number of messages to track. " +
      "For memory efficiency the consumer only tracks messages when necessary, the total number of messages being tracked " +
      "is roughly max.tracked.messages / percent of large messages * 100. Setting this number to be too small may result " +
      "in an exception when the user seeks backwards.";

  private static final String EXCEPTION_ON_MESSAGE_DROPPED_DOC = "The message assembler will drop message when buffer is " +
      "full or the incomplete message has expired. The consumer will throw a LargeMessageDroppedException if this " +
      "configuration is set to true. Otherwise the consumer will drop the message silently.";

  private static final String KEY_DESERIALIZER_CLASS_DOC = "The key deserializer class for the consumer.";

  private static final String VALUE_DESERIALIZER_CLASS_DOC = "The value deserializer class for the consumer.";

  private static final String SEGMENT_DESERIALIZER_CLASS_DOC = "The class used to deserialize the large message segments.";

  private static final String AUDITOR_CLASS_DOC = "The auditor class to use for the consumer";

  private static final String ENABLE_AUTO_COMMIT_DOC = "If true the consumer's offset will be periodically committed in" +
      " the background.";

  private static final String AUTO_COMMIT_INTERVAL_MS_DOC = "The frequency in milliseconds that the consumer offsets are" +
      " auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>.";

  private static final String AUTO_OFFSET_RESET_DOC = "What to do when there is no initial offset in Kafka or if the "
      + "current offset does not exist any more on the server (e.g. because that data has been deleted): "
      + "<ul><li>earliest: automatically reset the offset to the earliest offset<li>latest: automatically reset "
      + "the offset to the latest offset</li><li>none: throw exception to the consumer if no previous offset is found "
      + "for the consumer's group</li><li>anything else: throw exception to the consumer.</li></ul>";

  private static final String MAX_POLL_RECORDS_DOC = "The maximum number of records returned in a single call to poll().";

  public static final String METADATA_SERVICE_CLIENT_CLASS_DOC =
      LiKafkaCommonClientConfigs.METADATA_SERVICE_CLIENT_CLASS_DOC;

  public static final String METADATA_SERVICE_REQUEST_TIMEOUT_MS_DOC =
      LiKafkaCommonClientConfigs.METADATA_SERVICE_REQUEST_TIMEOUT_MS_DOC;

  public static final String CLUSTER_GROUP_DOC = LiKafkaCommonClientConfigs.CLUSTER_GROUP_DOC;

  public static final String CLUSTER_ENVIRONMENT_DOC = LiKafkaCommonClientConfigs.CLUSTER_ENVIRONMENT_DOC;

  static {
    CONFIG = new ConfigDef()
        .define(MESSAGE_ASSEMBLER_BUFFER_CAPACITY_CONFIG,
                ConfigDef.Type.INT,
                "32000000",
                Range.atLeast(1),
                Importance.MEDIUM,
                MESSAGE_ASSEMBLER_BUFFER_CAPACITY_DOC)
        .define(MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_CONFIG,
                Type.INT,
                "1000",
                Range.atLeast(1),
                Importance.MEDIUM,
                MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_DOC)
        .define(MAX_TRACKED_MESSAGES_PER_PARTITION_CONFIG,
                Type.INT,
                "500",
                Range.atLeast(1),
                Importance.MEDIUM,
                MAX_TRACKED_MESSAGES_PER_PARTITION_DOC)
        .define(EXCEPTION_ON_MESSAGE_DROPPED_CONFIG,
                Type.BOOLEAN,
                "false",
                Importance.LOW,
                EXCEPTION_ON_MESSAGE_DROPPED_DOC)
        .define(KEY_DESERIALIZER_CLASS_CONFIG,
                Type.CLASS,
                ByteArrayDeserializer.class.getName(),
                Importance.HIGH,
                KEY_DESERIALIZER_CLASS_DOC)
        .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                Type.CLASS,
                ByteArrayDeserializer.class.getName(),
                Importance.HIGH,
                VALUE_DESERIALIZER_CLASS_DOC)
        .define(SEGMENT_DESERIALIZER_CLASS_CONFIG,
                Type.CLASS,
                DefaultSegmentDeserializer.class.getName(),
                Importance.HIGH,
                SEGMENT_DESERIALIZER_CLASS_DOC)
        .define(AUDITOR_CLASS_CONFIG,
                Type.CLASS,
                NoOpAuditor.class.getName(),
                Importance.LOW,
                AUDITOR_CLASS_DOC)
        .define(ENABLE_AUTO_COMMIT_CONFIG,
                Type.BOOLEAN,
                "true",
                Importance.LOW,
                ENABLE_AUTO_COMMIT_DOC)
        .define(AUTO_COMMIT_INTERVAL_MS_CONFIG,
                Type.INT,
                "30000",
                Importance.LOW,
                AUTO_COMMIT_INTERVAL_MS_DOC)
        .define(AUTO_OFFSET_RESET_CONFIG,
                Type.STRING,
                "none",
                Importance.MEDIUM,
                AUTO_OFFSET_RESET_DOC)
        .define(MAX_POLL_RECORDS_CONFIG,
                Type.INT,
                500,
                atLeast(1),
                Importance.MEDIUM,
                MAX_POLL_RECORDS_DOC)
        .define(CommonClientConfigs.METADATA_MAX_AGE_CONFIG,
                Type.INT,
                5 * 60 * 1000,
                atLeast(0),
                Importance.MEDIUM,
                CommonClientConfigs.METADATA_MAX_AGE_DOC)
        .define(METADATA_SERVICE_CLIENT_CLASS_CONFIG,
                Type.CLASS,
                null,
                Importance.MEDIUM,
                METADATA_SERVICE_CLIENT_CLASS_DOC)
        .define(CLUSTER_GROUP_CONFIG,
                Type.STRING,
                "",
                Importance.MEDIUM,
                CLUSTER_GROUP_DOC)
        .define(CLUSTER_ENVIRONMENT_CONFIG,
                Type.STRING,
                "",
                Importance.MEDIUM,
                CLUSTER_ENVIRONMENT_DOC)
        .define(METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG,
                Type.INT,
                Integer.MAX_VALUE,
                Importance.MEDIUM,
                METADATA_SERVICE_REQUEST_TIMEOUT_MS_DOC)
        .define(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                Type.INT,
                60 * 1000,
                atLeast(0),
                Importance.MEDIUM,
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_DOC);
  }

  public LiKafkaConsumerConfig(Map<?, ?> props) {
    super(CONFIG, props, false);
  }

  /**
   * This method returns the configurations that are going to be used by the vanilla open source Kafka Consumer.
   * It disables auto commit and change the offset reset strategy to be NONE.
   */
  Map<String, Object> configForVanillaConsumer() {
    Map<String, Object> newConfigs = new HashMap<>();
    newConfigs.putAll(this.originals());
    newConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    newConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    return newConfigs;
  }

}
