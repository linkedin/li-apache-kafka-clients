/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage.errors;

/**
 * The exception is thrown when a large message send failed.
 */
public class LargeMessageSendException extends LargeMessageException {

  public LargeMessageSendException(String message, Throwable cause) {
    super(message, cause);
  }

  public LargeMessageSendException(String message) {
    super(message);
  }

}
