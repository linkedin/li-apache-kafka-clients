/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.util.UUID;


/**
 * Generate a UUID by calling the default java UUID generator which can have awful performance.
 */
public class UUIDFactoryImpl implements UUIDFactory {

  @Override
  public UUID create() {
    return UUID.randomUUID();
  }
}
