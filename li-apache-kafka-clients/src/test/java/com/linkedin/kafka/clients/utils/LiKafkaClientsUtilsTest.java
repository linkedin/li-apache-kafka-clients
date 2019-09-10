/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LiKafkaClientsUtilsTest {

  @Test
  public void testGetKnownLibraryVersions() throws Exception {
    Map<String, String> versions = LiKafkaClientsUtils.getKnownLibraryVersions();
    Assert.assertEquals(versions.size(), 3);
    Assert.assertNotNull(versions.get("com.linkedin.kafka"));
    Assert.assertNotNull(versions.get("com.linkedin.mario"));
    //if we run from an IDE we wont get a version for "com.linkedin.kafka.clients"
  }
}
