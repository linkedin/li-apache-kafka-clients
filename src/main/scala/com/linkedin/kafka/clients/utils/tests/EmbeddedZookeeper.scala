/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.utils.tests

import java.net.InetSocketAddress

import kafka.utils.CoreUtils
import org.apache.kafka.common.utils.Utils
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

/**
 * This is a copy of open source Kafka embedded zookeeper class. We put it here to avoid dependency on o.a.k.test jar.
 */
class EmbeddedZookeeper() {
  val snapshotDir = TestUtils.tempDir()
  val logDir = TestUtils.tempDir()
  val tickTime = 500
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val factory = new NIOServerCnxnFactory()
  private val addr = new InetSocketAddress("127.0.0.1", TestUtils.RandomPort)
  factory.configure(addr, 0)
  factory.startup(zookeeper)
  val port = zookeeper.getClientPort

  def shutdown() {
    CoreUtils.swallow(zookeeper.shutdown())
    CoreUtils.swallow(factory.shutdown())
    Utils.delete(logDir)
    Utils.delete(snapshotDir)
  }

}
