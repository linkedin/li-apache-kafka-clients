/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

public abstract class AbstractZookeeperTestHarness {
  protected EmbeddedZookeeper _zookeeper = null;

  public void setUp() {
    if (_zookeeper == null) {
      _zookeeper = new EmbeddedZookeeper();
    }
  }

  public void tearDown() {
    if (_zookeeper != null) {
      KafkaTestUtils.quietly(() -> _zookeeper.close());
      _zookeeper = null;
    }
  }

  protected EmbeddedZookeeper zookeeper() {
    return _zookeeper;
  }
}
