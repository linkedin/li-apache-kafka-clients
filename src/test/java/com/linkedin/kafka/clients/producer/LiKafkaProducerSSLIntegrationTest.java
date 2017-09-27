/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import java.io.File;
import java.io.IOException;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.testng.Assert;


public class LiKafkaProducerSSLIntegrationTest extends LiKafkaProducerIntegrationTest {

  private File _trustStoreFile;

  public LiKafkaProducerSSLIntegrationTest() {
    super();
    try {
      _trustStoreFile = File.createTempFile("truststore", ".jks");
    } catch (IOException e) {
      Assert.fail("Failed to create trust store");
    }
  }

  @Override
  public File trustStoreFile() {
    return _trustStoreFile;
  }

  @Override
  public SecurityProtocol securityProtocol() {
    return SecurityProtocol.SSL;
  }

}
