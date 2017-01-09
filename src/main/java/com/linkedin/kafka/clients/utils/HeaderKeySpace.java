/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
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
