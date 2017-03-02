/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.headers;
/**
 * Header key related constants and utilities.
 */
public class HeaderUtils {

  private HeaderUtils() {
    //This does nothing
  }

  public static final int MAX_KEY_LENGTH = Byte.MAX_VALUE;

  public static final String LARGE_MESSAGE_SEGMENT_HEADER = "li.lms";

  public static boolean isKeyValid(String headerKey) {
    return headerKey != null && headerKey.length() < MAX_KEY_LENGTH && headerKey.length() > 0;
  }

  public static void validateHeaderKey(String headerKey) {
    if (!isKeyValid(headerKey)) {
      throw new IllegalArgumentException("Invalid header key " + headerKey + ".");
    }
  }

}
