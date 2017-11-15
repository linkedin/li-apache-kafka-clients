/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage.errors;

/**
 * This exception is thrown when a {@link com.linkedin.kafka.clients.largemessage.LargeMessageSegment} contains
 * invalid fields. This exception can also be thrown when several individually valid segments cannot be assembled to
 * a complete large message.
 */
public class InvalidSegmentException extends LargeMessageException {

  public InvalidSegmentException(String message) {
    super(message);
  }

}
