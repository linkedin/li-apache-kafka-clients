/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;


public class EmbeddedZookeeper implements AutoCloseable {
  private File snapshotDir;
  private File logDir;
  private int tickTime;
  private String hostAddress;
  private int port;
  private ZooKeeperServer zk;
  private ServerCnxnFactory cnxnFactory;

  public EmbeddedZookeeper() {
    try {
      snapshotDir = KafkaTestUtils.newTempDir();
      logDir = KafkaTestUtils.newTempDir();
      tickTime = 500;
      zk = new ZooKeeperServer(snapshotDir, logDir, tickTime);
      cnxnFactory = new NIOServerCnxnFactory();
      InetAddress localHost = InetAddress.getLocalHost();
      hostAddress = localHost.getHostAddress();
      InetSocketAddress bindAddress = new InetSocketAddress(localHost, port);
      cnxnFactory.configure(bindAddress, 0);
      cnxnFactory.startup(zk);
      port = zk.getClientPort();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    //sanity check
    if (zk.getClientPort() != port) {
      throw new IllegalStateException();
    }
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public int getPort() {
    return port;
  }

  public String getConnectionString() {
    return hostAddress + ":" + port;
  }

  @Override
  public void close() throws Exception {
    KafkaTestUtils.quietly(() -> zk.shutdown());
    KafkaTestUtils.quietly(() -> cnxnFactory.shutdown());
  }
}
