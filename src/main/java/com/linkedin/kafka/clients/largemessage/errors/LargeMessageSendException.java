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
