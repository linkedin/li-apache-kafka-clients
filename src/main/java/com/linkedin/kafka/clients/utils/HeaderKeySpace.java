/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kafka.clients.utils;

/**
 * This determines the header key range used for various purposes.
 *
 */
public class HeaderKeySpace {

  private HeaderKeySpace() {
    //This does nothing
  }

  public static final int LIKAFKA_PRIVATE_START = 0;

  public static final int LARGE_MESSAGE_SEGMENT_HEADER = 2;

  public static final int PUBLIC_ASSIGNED_START = 10_000;

  public static final int PUBLIC_UNASSIGNED_START = 10_000_000;

  public static boolean isKeyValid(int headerKey) {
    return headerKey >= 0;
  }

  public static boolean isKeyInPrivateRange(int headerKey) {
    return headerKey >= LIKAFKA_PRIVATE_START && headerKey < PUBLIC_ASSIGNED_START;
  }
}
