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
