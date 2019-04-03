/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClient;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClientException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The unit test for federated consumer.
 */
public class LiKafkaFederatedConsumerImplTest {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaFederatedConsumerImplTest.class);

  private static final UUID CLIENT_ID = new UUID(0, 0);
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";
  private static final String TOPIC3 = "topic3";
  private static final TopicPartition TOPIC_PARTITION1 = new TopicPartition(TOPIC1, 0);
  private static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC2, 0);
  private static final TopicPartition TOPIC_PARTITION3 = new TopicPartition(TOPIC3, 0);
  private static final ClusterDescriptor CLUSTER1 = new ClusterDescriptor("cluster1", "url1", "zk1");
  private static final ClusterDescriptor CLUSTER2 = new ClusterDescriptor("cluster2", "url2", "zk2");
  private static final ClusterGroupDescriptor CLUSTER_GROUP = new ClusterGroupDescriptor("group", "env");

  private MetadataServiceClient _mdsClient;
  private LiKafkaFederatedConsumerImpl<byte[], byte[]> _federatedConsumer;

  private class MockConsumerBuilder extends LiKafkaConsumerBuilder<byte[], byte[]> {
    OffsetResetStrategy _offsetResetStrategy = OffsetResetStrategy.EARLIEST;

    public MockConsumerBuilder setOffsetResetStrategy(OffsetResetStrategy offsetResetStrategy) {
      _offsetResetStrategy = offsetResetStrategy;
      return this;
    }

    @Override
    public LiKafkaConsumer<byte[], byte[]> build() {
      return new MockLiKafkaConsumer(_offsetResetStrategy);
    }
  }

  @BeforeMethod
  public void setup() {
    _mdsClient = Mockito.mock(MetadataServiceClient.class);
    when(_mdsClient.registerFederatedClient(anyObject(), anyObject(), anyInt())).thenReturn(CLIENT_ID);

    Map<String, String> consumerConfig = new HashMap<>();
    consumerConfig.put(LiKafkaConsumerConfig.CLUSTER_GROUP_CONFIG, CLUSTER_GROUP.getName());
    consumerConfig.put(LiKafkaConsumerConfig.CLUSTER_ENVIRONMENT_CONFIG, CLUSTER_GROUP.getEnvironment());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    _federatedConsumer = new LiKafkaFederatedConsumerImpl<>(consumerConfig, _mdsClient, new MockConsumerBuilder());
  }

  @Test
  public void testBasicWorkflow() throws MetadataServiceClientException {
    // Set expectations so that topics 1 and 3 are hosted in cluster 1 and topic 2 in cluster 2.
    Collection<TopicPartition> expectedTopicPartitions = Arrays.asList(TOPIC_PARTITION1, TOPIC_PARTITION2,
        TOPIC_PARTITION3);
    Map<TopicPartition, ClusterDescriptor> topicPartitionsToClusterMapToReturn =
        new HashMap<TopicPartition, ClusterDescriptor>() {{
          put(TOPIC_PARTITION1, CLUSTER1);
          put(TOPIC_PARTITION2, CLUSTER2);
          put(TOPIC_PARTITION3, CLUSTER1);
    }};
    when(_mdsClient.getClustersForTopicPartitions(eq(CLIENT_ID), eq(expectedTopicPartitions), eq(CLUSTER_GROUP),
        anyInt())).thenReturn(topicPartitionsToClusterMapToReturn);

    // Make sure we start with a clean slate
    assertNull("Consumer for cluster 1 should have not been created yet",
        _federatedConsumer.getPerClusterConsumer(CLUSTER1));
    assertNull("Consumer for cluster 2 should have not been created yet",
        _federatedConsumer.getPerClusterConsumer(CLUSTER2));

    // Assign topic partitions from all three topics
    _federatedConsumer.assign(Arrays.asList(TOPIC_PARTITION1, TOPIC_PARTITION2, TOPIC_PARTITION3));

    // Verify consumers for both clusters have been created.
    MockConsumer consumer1 = ((MockLiKafkaConsumer) _federatedConsumer.getPerClusterConsumer(CLUSTER1)).getDelegate();
    MockConsumer consumer2 = ((MockLiKafkaConsumer) _federatedConsumer.getPerClusterConsumer(CLUSTER2)).getDelegate();
    assertNotNull("Consumer for cluster 1 should have been created", consumer1);
    assertNotNull("Consumer for cluster 2 should have been created", consumer2);

    // Verify if assignment() returns all topic partitions.
    assertEquals("assignment() should return all topic partitions",
        new HashSet<TopicPartition>(expectedTopicPartitions), _federatedConsumer.assignment());

    // Set beginning offsets for each per-cluster consumer (needed for MockConsumer).
    HashMap<TopicPartition, Long> beginningOffsets1 = new HashMap<>();
    beginningOffsets1.put(TOPIC_PARTITION1, 0L);
    beginningOffsets1.put(TOPIC_PARTITION3, 0L);
    consumer1.updateBeginningOffsets(beginningOffsets1);

    HashMap<TopicPartition, Long> beginningOffsets2 = new HashMap<>();
    beginningOffsets2.put(TOPIC_PARTITION2, 0L);
    consumer2.updateBeginningOffsets(beginningOffsets2);

    // Prepare test setup where one record for topic1, two records for topic2, and one record for topic3 are available
    // for consumption.
    ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord(TOPIC_PARTITION1.topic(), TOPIC_PARTITION1.partition(),
        0, "key1", "value1");
    ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord(TOPIC_PARTITION2.topic(), TOPIC_PARTITION2.partition(),
        0, "key2", "value2");
    ConsumerRecord<byte[], byte[]> record3 = new ConsumerRecord(TOPIC_PARTITION2.topic(), TOPIC_PARTITION2.partition(),
        1, "key4", "value4");
    ConsumerRecord<byte[], byte[]> record4 = new ConsumerRecord(TOPIC_PARTITION3.topic(), TOPIC_PARTITION3.partition(),
        0, "key3", "value3");

    consumer1.addRecord(record1);
    consumer1.addRecord(record4);

    consumer2.addRecord(record2);
    consumer2.addRecord(record3);

    // Poll the federated consumer and verify the result.
    ConsumerRecords<byte[], byte[]> pollResult = _federatedConsumer.poll(1000);
    assertEquals("poll should return four records", 4, pollResult.count());
    assertEquals("poll should return one record for topic1", new ArrayList<>(Arrays.asList(record1)), pollResult.records(TOPIC_PARTITION1));
    assertEquals("poll should return two records for topic2", new ArrayList<>(Arrays.asList(record2, record3)), pollResult.records(TOPIC_PARTITION2));
    assertEquals("poll should return one record for topic3", new ArrayList<>(Arrays.asList(record4)), pollResult.records(TOPIC_PARTITION3));

    // Verify per-cluster consumers are not in closed state.
    assertFalse("Consumer for cluster 1 should have not been closed", consumer1.closed());
    assertFalse("Consumer for cluster 2 should have not been closed", consumer2.closed());

    // Close the federated consumer and verify both consumers are closed.
    _federatedConsumer.close();
    assertTrue("Consumer for cluster 1 should have been closed", consumer1.closed());
    assertTrue("Consumer for cluster 2 should have been closed", consumer2.closed());
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
