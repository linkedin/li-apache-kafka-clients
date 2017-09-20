/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

public abstract class AbstractZookeeperTestHarness {
  protected EmbeddedZookeeper zookeeper = null;

  public void setUp() {
    if (zookeeper == null) {
      zookeeper = new EmbeddedZookeeper();
    }
  }

  public void tearDown() {
    if (zookeeper != null) {
      KafkaTestUtils.quietly(() -> zookeeper.close());
      zookeeper = null;
    }
  }
}
