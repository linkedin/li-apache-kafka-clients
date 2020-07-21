/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.security;

import java.util.Map;


/**
 * Interface that helps retrieve and/or manage per-topic encrypter-decrypter instances.
 *
 * It is left upto the implementing class on whether the encrypter-decrypter instance is unique per-topic or if it is a
 * singleton shared for all topics.
 */
public interface TopicEncrypterDecrypterManager {
  /**
   * Method used to configure this instance of the TopicEncrypterDecrypterManager.
   *
   * This method is particularly useful when components need to be dynamically loaded by name during runtime. It is
   * typically invoked as soon as the instance is constructed and prior to any calls to
   * {@link TopicEncrypterDecrypterManager#getEncrypterDecrypter(String topic)}
   *
   * @param configs Required for managing the encrypter-decrypter instances. Typical configurations include those
   *                necessary to connect with a key-management server to fetch the keys for encryption/decryption. It
   *                could include additional configurations that are specific to the implementing class.
   */
  void configure(Map<String, ?> configs);

  /**
   * Method used to retrieve the {@link KafkaMessageEncrypterDecrypter} instance associated with a given topic. It is
   * left upto the implementing class on whether the {@link KafkaMessageEncrypterDecrypter} instance returned is a
   * singleton or unique per-topic.
   *
   * @param topic String representing the name of the topic
   * @return Returns a non-null instance of {@link KafkaMessageEncrypterDecrypter}
   */
  KafkaMessageEncrypterDecrypter getEncrypterDecrypter(String topic);
}
