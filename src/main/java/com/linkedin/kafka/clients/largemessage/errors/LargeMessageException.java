/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
