/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

public class Constants {
  // The variables reserved by kafka for auditing purpose
  public static final String TIMESTAMP_HEADER = "_t";
  public static final String LARGE_MESSAGE_HEADER = "_lm";

  /**
   * Avoid instantiating the constants class
   */
  private Constants() {

  }
}
