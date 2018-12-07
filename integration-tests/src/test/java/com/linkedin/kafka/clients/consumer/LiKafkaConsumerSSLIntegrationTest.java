/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import java.io.File;
import java.io.IOException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class LiKafkaConsumerSSLIntegrationTest extends LiKafkaConsumerIntegrationTest {

  private File _trustStoreFile;

  public LiKafkaConsumerSSLIntegrationTest() {
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
