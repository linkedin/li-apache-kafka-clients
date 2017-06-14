/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.largemessage.DefaultSegmentDeserializer;
import com.linkedin.kafka.clients.auditing.NoOpAuditor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.HashMap;
import java.util.Map;

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
  public static final String SKIP_RECORD_ON_EXCEPTION_CONFIG = "skip.record.on.exception";

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

  private static final String SKIP_RECORD_ON_EXCEPTION_DOC = "Skip the record with processing error. This is to allow "
      + "the users who do are willing to ignore the problematic record to continue consuming without worrying about "
      + "the error handling. By default it is set to false and an exception will be thrown if record processing "
      + "encounters an error. If user ignore the exception and call poll again, the error message will also be skipped.";

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
        .define(SKIP_RECORD_ON_EXCEPTION_CONFIG,
                Type.BOOLEAN,
                "false",
                Importance.LOW,
                SKIP_RECORD_ON_EXCEPTION_DOC);

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
