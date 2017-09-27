/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.testng.Assert;


public abstract class AbstractKafkaIntegrationTestHarness extends AbstractZookeeperTestHarness {
  protected Map<String, EmbeddedBroker> _brokers = null;
  protected String _bootstrapUrl;

  @Override
  public void setUp() {
    super.setUp();
    if (_brokers != null) {
      return;
    }

    _brokers = new LinkedHashMap<>();
    List<Map<Object, Object>> brokerConfigs = buildBrokerConfigs();
    Assert.assertNotNull(brokerConfigs);
    Assert.assertFalse(brokerConfigs.isEmpty());
    for (Map<Object, Object> brokerConfig : brokerConfigs) {
      EmbeddedBroker broker = new EmbeddedBroker(brokerConfig);
      String id = broker.getId();
      if (_brokers.putIfAbsent(id, broker) != null) {
        KafkaTestUtils.quietly(broker::close); //wont be picked up by teardown
        throw new IllegalStateException("multiple brokers defined with id " + id);
      }
    }

    StringJoiner joiner = new StringJoiner(",");
    _brokers.values().forEach(broker -> joiner.add(broker.getAddr(securityProtocol())));
    _bootstrapUrl = joiner.toString();
  }

  @Override
  public void tearDown() {
    try {
      if (_brokers != null) {
        for (EmbeddedBroker broker : _brokers.values()) {
          KafkaTestUtils.quietly(broker::close);
        }
        _brokers.clear();
        _brokers = null;
      }
    } finally {
      super.tearDown();
    }
  }

  protected EmbeddedBroker serverForId(int id) {
    return broker(Integer.toString(id));
  }

  protected EmbeddedBroker broker(String id) {
    EmbeddedBroker broker = _brokers.get(id);
    if (broker == null) {
      throw new IllegalArgumentException("Invalid server id " + id);
    }
    return broker;
  }

  public String bootstrapServers() {
    return _bootstrapUrl;
  }

  /**
   * returns the list of broker configs for all brokers created by this test
   * (as determined by clusterSize()
   * @return list of broker configs, one config map per broker to be created
   */
  protected List<Map<Object, Object>> buildBrokerConfigs() {
    List<Map<Object, Object>> configs = new ArrayList<>();
    for (int i = 0; i < clusterSize(); i++) {
      EmbeddedBrokerBuilder builder = new EmbeddedBrokerBuilder();
      builder.zkConnect(zookeeper());
      builder.nodeId(i);
      builder.enable(securityProtocol());
      if (securityProtocol() == SecurityProtocol.SSL) {
        if (trustStoreFile() != null) {
          builder.trustStore(trustStoreFile());
        }
      } else {
        if (trustStoreFile() != null) {
          throw new AssertionError("security protocol not yet yet trust store file provided");
        }
      }
      Map<Object, Object> config = builder.buildConfig();
      config.putAll(overridingProps());
      configs.add(config);
    }
    return configs;
  }

  protected SecurityProtocol securityProtocol() {
    return SecurityProtocol.PLAINTEXT;
  }

  protected File trustStoreFile() {
    return null;
  }

  protected int clusterSize() {
    return 1;
  }

  protected Map<Object, Object> overridingProps() {
    return Collections.emptyMap();
  }
}
