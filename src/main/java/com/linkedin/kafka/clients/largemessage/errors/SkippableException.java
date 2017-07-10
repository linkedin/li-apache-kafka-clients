/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage.errors;

public class SkippableException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public SkippableException() {
    super();
  }

  public SkippableException(String message, Throwable cause) {
    super(message, cause);
  }

  public SkippableException(String message) {
    super(message);
  }

  public SkippableException(Throwable cause) {
    super(cause);
  }

}
