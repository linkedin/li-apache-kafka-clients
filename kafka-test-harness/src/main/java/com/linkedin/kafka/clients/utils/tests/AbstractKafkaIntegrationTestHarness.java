/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.kafka.common.security.auth.SecurityProtocol;


public abstract class AbstractKafkaIntegrationTestHarness extends AbstractZookeeperTestHarness {
  private final static Random RANDOM = new Random();
  protected Map<Integer, EmbeddedBroker> _brokers = null;
  protected Set<Integer> _deadBrokers;
  protected String _bootstrapUrl;

  @Override
  public void setUp() {
    super.setUp();
    if (_brokers != null) {
      return;
    }

    _brokers = new LinkedHashMap<>();
    List<Map<Object, Object>> brokerConfigs = buildBrokerConfigs();
    if (brokerConfigs == null || brokerConfigs.isEmpty()) {
      throw new AssertionError("Broker configs " + brokerConfigs + " should not be null or empty");
    }
    for (Map<Object, Object> brokerConfig : brokerConfigs) {
      EmbeddedBroker broker = new EmbeddedBroker(brokerConfig);
      int id = broker.getId();
      if (_brokers.putIfAbsent(id, broker) != null) {
        KafkaTestUtils.quietly(broker::close); //wont be picked up by teardown
        throw new IllegalStateException("multiple brokers defined with id " + id);
      }
    }

    StringJoiner joiner = new StringJoiner(",");
    _brokers.values().forEach(broker -> joiner.add(broker.getAddr(securityProtocol())));
    _bootstrapUrl = joiner.toString();
    _deadBrokers = new HashSet<>();
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
    return broker(id);
  }

  protected EmbeddedBroker broker(int id) {
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
          throw new AssertionError("security protocol not set yet trust store file provided");
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

  /**
   * Kill broker by broker id
   * @param id id of broker
   * @throws Exception if anything goes wrong
   */
  public void killBroker(int id) throws Exception {
    EmbeddedBroker broker = _brokers.get(id);

    if (!_deadBrokers.contains(id)) {
      broker.shutdown();
      broker.awaitShutdown();
      _deadBrokers.add(id);
    }
  }

  /**
   * Kill a random broker that is not alive.
   *
   * @return id of broker killed
   * @throws Exception if anything goes wrong
   */
  public int killRandomBroker() throws Exception {
    int index = RANDOM.nextInt(_brokers.size());
    int id = (Integer) _brokers.keySet().toArray()[index];
    killBroker(id);
    return id;
  }

  /**
   * Restart all dead brokers
   * @return Returns a list of brokers that were restarted
   * @throws Exception all exceptions caused while starting brokers
   */
  public List<Integer> restartDeadBrokers() throws Exception {
    List<Integer> brokersStarted = new ArrayList<>();
    for (int id : _deadBrokers) {
      _brokers.get(id).startup();
      brokersStarted.add(id);
    }
    _deadBrokers.clear();
    return brokersStarted;
  }
}
