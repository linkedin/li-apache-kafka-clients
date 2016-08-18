/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage.errors;

/**
 * Thrown when a message is dropped from the message pool due to buffer full.
 */
public class LargeMessageDroppedException extends LargeMessageException {

  public LargeMessageDroppedException(String message) {
    super(message);
  }

}
