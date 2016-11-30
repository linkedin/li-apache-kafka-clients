/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.auditing.NoOpAuditor;
import com.linkedin.kafka.clients.producer.LiKafkaProducer;
import com.linkedin.kafka.clients.utils.HeaderParser;
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
  public static final String AUDITOR_CLASS_CONFIG = "auditor.class";
  public static final String LI_KAFKA_MAGIC_CONFIG = "likafka.magic";
  public static final String ENABLE_AUTO_COMMIT_CONFIG = ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
  public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;

  public static final String LI_KAFKA_MAGIC_DOC = "Configures the magic number that is used to distinguish a " +
    " message produced by " + LiKafkaProducer.class.getName() + ".  Probably you don't want to configure this unless you " +
    " are getting collisions between the default magic number and your data.  Format is hexadecimal string. No 0x prefix.";

  public static final String MESSAGE_ASSEMBLER_BUFFER_CAPACITY_DOC = "The maximum number of bytes the message assembler " +
      " uses to buffer the incomplete large message segments. The capacity is shared by messages from all the topics. " +
      "If the capacity of the message assembler has been reached, the consumer will drop the oldest incomplete message " +
      "in the buffer.";

  public static final String MESSAGE_ASSEMBLER_EXPIRATION_OFFSET_GAP_DOC = "The message assembler will expire and " +
      "discard the buffered large message segments of an incomplete large message if that message has sit in the buffer " +
      "for too long. The expiration is based on the difference between current consumer offset of the partition and the " +
      "starting offset of the incomplete large message. When the current consumer offset of the partition is greater " +
      "than the starting offset of the incomplete large message + the value of this configuration, the incomplete large " +
      "message will be removed. The consumer may throw an exception depending on whether the user has set " +
      "exception.on.message.dropped to true or false.";

  public static final String MAX_TRACKED_MESSAGES_PER_PARTITION_DOC = "In order to support large messages, LiKafkaConsumer " +
      "keeps track of the messages that are delivered. This configuration sets the maximum number of messages to track. " +
      "For memory efficiency the consumer only tracks messages when necessary, the total number of messages being tracked " +
      "is roughly max.tracked.messages / percent of large messages * 100. Setting this number to be too small may result " +
      "in an exception when the user seeks backwards.";

  public static final String EXCEPTION_ON_MESSAGE_DROPPED_DOC = "The message assembler will drop message when buffer is " +
      "full or the incomplete message has expired. The consumer will throw a LargeMessageDroppedException if this " +
      "configuration is set to true. Otherwise the consumer will drop the message silently.";

  public static final String KEY_DESERIALIZER_CLASS_DOC = "The key deserializer class for the consumer.";

  public static final String VALUE_DESERIALIZER_CLASS_DOC = "The value deserializer class for the consumer.";

  public static final String AUDITOR_CLASS_DOC = "The auditor class to use for the consumer";

  public static final String ENABLE_AUTO_COMMIT_DOC = "If true the consumer's offset will be periodically committed in" +
      " the background.";

  public static final String AUTO_COMMIT_INTERVAL_MS_DOC = "The frequency in milliseconds that the consumer offsets are" +
      " auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>.";

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
        .define(LI_KAFKA_MAGIC_CONFIG,
                Type.STRING,
                HeaderParser.defaultMagicAsString(),
                Importance.LOW,
                LI_KAFKA_MAGIC_DOC);


  }

  public LiKafkaConsumerConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }

  Map<String, Object> configWithAutoCommitDisabled() {
    Map<String, Object> newConfigs = new HashMap<>();
    newConfigs.putAll(this.originals());
    newConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    return newConfigs;
  }

}
