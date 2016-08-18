/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests

import javax.security.auth.login.Configuration
import kafka.utils.{ZkUtils, Logging, CoreUtils}
import kafka.zk.ZkFourLetterWords
import org.apache.kafka.common.security.JaasUtils

/**
 * Zookeeper test harness.
 * This is simply a copy of open source code, we do this because java does not support trait, we are making it abstract
 * class so user java test class can extend it.
 */
abstract class AbstractZookeeperTestHarness extends Logging {

  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  var zkUtils: ZkUtils = null
  var zookeeper: EmbeddedZookeeper = null

  def zkPort: Int = zookeeper.port

  def zkConnect: String = s"127.0.0.1:$zkPort"

  def setUp() {
    zookeeper = new EmbeddedZookeeper()
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, JaasUtils.isZkSecurityEnabled)
  }

  def tearDown() {
    if (zkUtils != null)
      CoreUtils.swallow(zkUtils.close())
    if (zookeeper != null)
      CoreUtils.swallow(zookeeper.shutdown())

    def isDown: Boolean = {
      try {
        ZkFourLetterWords.sendStat("127.0.0.1", zkPort, 3000)
        false
      } catch {
        case _: Throwable =>
          debug("Server is down")
          true
      }
    }

    Iterator.continually(isDown).exists(identity)

    Configuration.setConfiguration(null)
  }

}
