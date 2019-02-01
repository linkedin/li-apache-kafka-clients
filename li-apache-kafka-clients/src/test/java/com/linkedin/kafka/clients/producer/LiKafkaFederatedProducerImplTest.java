/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

/**
 * The unit test for federated producer.
 */
public class LiKafkaFederatedProducerImplTest {
  private static final UUID CLIENT_ID = new UUID(0, 0);
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";
  private static final String TOPIC3 = "topic3";
  private static final ClusterDescriptor CLUSTER1 = new ClusterDescriptor("cluster1", "url1", "zk1");
  private static final ClusterDescriptor CLUSTER2 = new ClusterDescriptor("cluster2", "url2", "zk2");

  private MetadataServiceClient _mdsClient;
  private LiKafkaFederatedProducerImpl<byte[], byte[]> _federatedProducer;

  private class MockProducerBuilder extends RawProducerBuilder {
    @Override
    public Producer<byte[], byte[]> build() {
      return new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    }
  }

  @BeforeMethod
  public void setup() {
    _mdsClient = Mockito.mock(MetadataServiceClient.class);
    when(_mdsClient.registerFederatedClient(anyObject(), anyObject())).thenReturn(CLIENT_ID);

    Map<String, String> producerConfig = new HashMap<>();
    producerConfig.put(LiKafkaProducerConfig.CLUSTER_ENVIRONMENT_CONFIG, "env");
    producerConfig.put(LiKafkaProducerConfig.CLUSTER_GROUP_CONFIG, "group");
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

    _federatedProducer = new LiKafkaFederatedProducerImpl<>(producerConfig, null, null, null, _mdsClient,
        new MockProducerBuilder());
  }

  @Test
  public void testBasicWorkflow() {
    // Set expectations so that topics 1 and 3 are hosted in cluster 1 and topic 2 in cluster 2.
    when(_mdsClient.getClusterForTopic(eq(CLIENT_ID), eq(TOPIC1))).thenReturn(CLUSTER1);
    when(_mdsClient.getClusterForTopic(eq(CLIENT_ID), eq(TOPIC2))).thenReturn(CLUSTER2);
    when(_mdsClient.getClusterForTopic(eq(CLIENT_ID), eq(TOPIC3))).thenReturn(CLUSTER1);

    // Make sure we start with a clean slate
    assertNull("Producer for cluster 1 should have not been created yet",
        _federatedProducer.getPerClusterProducer(CLUSTER1));
    assertNull("Producer for cluster 2 should have not been created yet",
        _federatedProducer.getPerClusterProducer(CLUSTER2));

    // Produce to all three topics
    ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(TOPIC1, 0, 0L, "key1".getBytes(), "value1".getBytes());
    ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(TOPIC2, 0, 0L, "key2".getBytes(), "value2".getBytes());
    ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(TOPIC3, 0, 0L, "key3".getBytes(), "value3".getBytes());

    Future<RecordMetadata> metadata = _federatedProducer.send(record1);
    assertTrue("Send for topic 1 should be immediately completed", metadata.isDone());
    assertFalse("Send for topic 1 should be successful", isError(metadata));

    Future<RecordMetadata> metadata2 = _federatedProducer.send(record2);
    assertTrue("Send for topic 2 should be immediately completed", metadata2.isDone());
    assertFalse("Send for topic 2 should be successful", isError(metadata2));

    Future<RecordMetadata> metadata3 = _federatedProducer.send(record3);
    assertTrue("Send for topic 3 should be immediately completed", metadata3.isDone());
    assertFalse("Send for topic 3 should be successful", isError(metadata3));

    // Verify a correct producer is used for each send. Records 1 and 3 should be produced to cluster 1 and record 2 to
    // cluster 2.
    MockProducer<byte[], byte[]> producer1 = (MockProducer) _federatedProducer.getPerClusterProducer(CLUSTER1);
    MockProducer<byte[], byte[]> producer2 = (MockProducer) _federatedProducer.getPerClusterProducer(CLUSTER2);
    assertNotNull("Producer for cluster 1 should have been created", producer1);
    assertNotNull("Producer for cluster 2 should have been created", producer2);

    List<ProducerRecord> expectedHistory1 = new ArrayList<>(Arrays.asList(record1, record3));
    List<ProducerRecord> expectedHistory2 = new ArrayList<>(Arrays.asList(record2));
    assertEquals("Cluster1", expectedHistory1, producer1.history());
    assertEquals("Cluster2", expectedHistory2, producer2.history());

    // Verify per-cluster producers are not in closed state.
    assertFalse("Producer for cluster 1 should have not been closed", producer1.closed());
    assertFalse("Producer for cluster 2 should have not been closed", producer2.closed());

    // Close the federated producer and verify both producers are closed.
    _federatedProducer.close();
    assertTrue("Producer for cluster 1 should have been closed", producer1.closed());
    assertTrue("Producer for cluster 2 should have been closed", producer2.closed());
  }

  private boolean isError(Future<?> future) {
    try {
      future.get();
      return false;
    } catch (Exception e) {
      return true;
    }
  }
}
