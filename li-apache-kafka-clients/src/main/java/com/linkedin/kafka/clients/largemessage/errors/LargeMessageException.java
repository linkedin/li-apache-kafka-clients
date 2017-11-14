/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage.errors;

import org.apache.kafka.common.KafkaException;

/**
 * Exceptions for large messages.
 */
public abstract class LargeMessageException extends KafkaException {

  private static final long serialVersionUID = 1L;

  public LargeMessageException(String message, Throwable cause) {
    super(message, cause);
  }

  public LargeMessageException(String message) {
    super(message);
  }

  public LargeMessageException(Throwable cause) {
    super(cause);
  }
}
