/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.security.errors;

import org.apache.kafka.common.KafkaException;


/**
 * Exceptions for encryption and decryption.
 */

public class EncryptionException extends KafkaException {

  public EncryptionException(String message, Throwable cause) {
    super(message, cause);
  }

  public EncryptionException(String message) {
    super(message);
  }

  public EncryptionException(Throwable cause) {
    super(cause);
  }
}

