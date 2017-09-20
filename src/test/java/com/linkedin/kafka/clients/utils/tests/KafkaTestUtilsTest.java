/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.net.InetAddress;
import java.net.ServerSocket;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaTestUtilsTest {

  @Test
  public void testGetAvailableTcpPort() throws Exception {
    InetAddress loAddr = InetAddress.getLoopbackAddress();
    for (int i = 0; i < 100000; i++) { //enough ports to prove re-use
      int port = KafkaTestUtils.getAvailableTcpPort();
      Assert.assertTrue(port > 0);
      try (ServerSocket ss = new ServerSocket(port, 1, loAddr)) {
        Assert.assertEquals(port, ss.getLocalPort());
      }
    }
  }
}
