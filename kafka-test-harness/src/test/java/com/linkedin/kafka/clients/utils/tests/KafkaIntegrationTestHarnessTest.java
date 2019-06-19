package com.linkedin.kafka.clients.utils.tests;

import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class KafkaIntegrationTestHarnessTest {
  private static final int CLUSTER_SIZE = 4;
  private AbstractKafkaIntegrationTestHarness _kafkaIntegrationTestHarness;

  @BeforeTest
  public void setup() {
    _kafkaIntegrationTestHarness = new AbstractKafkaIntegrationTestHarness() {
      @Override
      protected int clusterSize() {
        return CLUSTER_SIZE;
      }
    };
    _kafkaIntegrationTestHarness.setUp();
  }

  @AfterTest
  public void teardown() {
    _kafkaIntegrationTestHarness.tearDown();
  }

  @Test
  public void testKillBroker() throws Exception {
    Set<Integer> brokerIds = _kafkaIntegrationTestHarness._brokers.keySet();
    Assert.assertFalse(brokerIds.isEmpty(), "broker not initialized");
    Assert.assertEquals(brokerIds.size(), CLUSTER_SIZE, "expected cluster size doesn't match the initialized brokers");

    int killedBrokerId = -1;
    for (Integer brokerId : brokerIds) {
      killedBrokerId = brokerId;
      _kafkaIntegrationTestHarness.killBroker(killedBrokerId);
      break;
    }

    List<Integer> restartedBrokers = _kafkaIntegrationTestHarness.restartDeadBrokers();
    Assert.assertEquals(restartedBrokers.size(), 1, "unexpected brokers restarted");
    Assert.assertTrue(restartedBrokers.contains(killedBrokerId), "broker restart is not the broker that was killed");
  }

  @Test
  public void testKillRandomBroker() throws Exception {
    Set<Integer> brokerIds = _kafkaIntegrationTestHarness._brokers.keySet();
    Assert.assertFalse(brokerIds.isEmpty(), "broker not initialized");
    Assert.assertEquals(brokerIds.size(), CLUSTER_SIZE, "expected cluster size doesn't match the initialized brokers");

    int killedBrokerId = _kafkaIntegrationTestHarness.killRandomBroker();

    List<Integer> restartedBrokers = _kafkaIntegrationTestHarness.restartDeadBrokers();
    Assert.assertEquals(restartedBrokers.size(), 1, "unexpected brokers restarted");
    Assert.assertTrue(restartedBrokers.contains(killedBrokerId), "broker restart is not the broker that was killed");
  }
}
