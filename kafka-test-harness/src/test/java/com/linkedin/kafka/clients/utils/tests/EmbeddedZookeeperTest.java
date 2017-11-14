/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.net.ConnectException;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;


public class EmbeddedZookeeperTest {

  @Test
  public void testSimpleScenario() throws Exception {
    String connectionString;
    String host;
    int port;
    try (EmbeddedZookeeper zk = new EmbeddedZookeeper()) {
      connectionString = zk.getConnectionString();
      host = zk.getHostAddress();
      port = zk.getPort();
      Assert.assertEquals(host + ":" + port, connectionString);
      ZkClient client = new ZkClient(connectionString);
      try {
        String path = "/" + UUID.randomUUID().toString();
        client.waitUntilConnected(5, TimeUnit.SECONDS);
        client.create(path, "payload", CreateMode.PERSISTENT);
        Assert.assertEquals("payload", client.readData(path));
      } finally {
        client.close();
      }
    }
    //now verify shut down
    try {
      new Socket(host, port);
      Assert.fail("expected to fail");
    } catch (ConnectException ignored) {

    }
  }
}
