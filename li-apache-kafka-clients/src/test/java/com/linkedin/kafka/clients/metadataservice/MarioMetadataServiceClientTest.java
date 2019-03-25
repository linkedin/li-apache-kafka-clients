/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.metadataservice;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.mario.client.MarioClient;
import com.linkedin.mario.client.models.v1.TopicQuery;
import com.linkedin.mario.common.models.v1.KafkaClusterDescriptor;
import com.linkedin.mario.common.models.v1.KafkaTopicModel;
import com.linkedin.mario.common.models.v1.TopicQueryResults;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The unit test for Mario metadata service client.
 */
public class MarioMetadataServiceClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(MarioMetadataServiceClientTest.class);

  private static final ClusterGroupDescriptor CLUSTER_GROUP = new ClusterGroupDescriptor("group", "env");
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";
  private static final ClusterDescriptor CLUSTER1 = new ClusterDescriptor("cluster1", "url1", "zk1");
  private static final ClusterDescriptor CLUSTER2 = new ClusterDescriptor("cluster2", "url2", "zk2");
  private static final UUID CLUSTER1_ID = new UUID(0, 1);
  private static final UUID CLUSTER2_ID = new UUID(0, 2);

  private MarioClient _marioClient;
  private MarioMetadataServiceClient _marioMetadataServiceClient;
  private UUID _clientId;

  private class TopicQueryMatcher implements ArgumentMatcher<TopicQuery> {
    private TopicQuery _query;

    public TopicQueryMatcher(TopicQuery query) {
      _query = query;
    }

    @Override
    public boolean matches(TopicQuery other) {
      return _query.getTopicNames().equals(other.getTopicNames()) &&
          _query.getClusterGroups().equals(other.getClusterGroups()) &&
          _query.getEnvironments().equals(other.getEnvironments());
    }
  }

  @BeforeMethod
  public void setup() {
    _marioClient = Mockito.mock(MarioClient.class);
    _marioMetadataServiceClient = new MarioMetadataServiceClient(_marioClient);
    _clientId = _marioMetadataServiceClient.registerFederatedClient(CLUSTER_GROUP, null, 1000);
  }

  @Test
  public void testGetClusterForTopic() throws MetadataServiceClientException {
    // Set expectations so that Mario would be queried for TOPIC1 and return CLUSTER1.
    TopicQuery expectedQuery = new TopicQuery(true, null, new HashSet<>(Arrays.asList(CLUSTER_GROUP.name())), null,
        new HashSet<>(Arrays.asList(CLUSTER_GROUP.environment())), new HashSet<>(Arrays.asList(TOPIC1)));

    List<KafkaClusterDescriptor> expectedClustersFromMario =
        Arrays.asList(new KafkaClusterDescriptor(CLUSTER1_ID, 0, CLUSTER1.name(), CLUSTER_GROUP.name(), "",
            CLUSTER1.zkConnection(), CLUSTER1.bootstrapURL(), CLUSTER_GROUP.environment()));
    List<KafkaTopicModel> expectedTopicsFromMario = Arrays.asList(new KafkaTopicModel(CLUSTER1_ID, 0, TOPIC1, 1));
    TopicQueryResults expectedTopicQueryResults =
        new TopicQueryResults(expectedClustersFromMario, expectedTopicsFromMario);
    CompletableFuture<TopicQueryResults> expectedFuture = new CompletableFuture();
    expectedFuture.complete(new TopicQueryResults(expectedClustersFromMario, expectedTopicsFromMario));

    when(_marioClient.queryTopics(argThat(new TopicQueryMatcher(expectedQuery)))).thenReturn(expectedFuture);

    assertEquals(CLUSTER1, _marioMetadataServiceClient.getClusterForTopic(_clientId, TOPIC1, 1000));
  }

  @Test
  public void testGetClustersForTopicPartitions() throws MetadataServiceClientException {
    // Set expectations so that Mario would be queried for TOPIC1 and TOPIC2 and return CLUSTER1 and CLUSTER2
    // respectively.
    TopicQuery expectedQuery = new TopicQuery(true, null, new HashSet<>(Arrays.asList(CLUSTER_GROUP.name())), null,
        new HashSet<>(Arrays.asList(CLUSTER_GROUP.environment())), new HashSet<>(Arrays.asList(TOPIC1, TOPIC2)));

    List<KafkaClusterDescriptor> expectedClustersFromMario =
        Arrays.asList(
            new KafkaClusterDescriptor(CLUSTER1_ID, 0, CLUSTER1.name(), CLUSTER_GROUP.name(), "",
            CLUSTER1.zkConnection(), CLUSTER1.bootstrapURL(), CLUSTER_GROUP.environment()),
            new KafkaClusterDescriptor(CLUSTER2_ID, 0, CLUSTER2.name(), CLUSTER_GROUP.name(), "",
                CLUSTER2.zkConnection(), CLUSTER2.bootstrapURL(), CLUSTER_GROUP.environment()));
    List<KafkaTopicModel> expectedTopicsFromMario =
        Arrays.asList(new KafkaTopicModel(CLUSTER1_ID, 0, TOPIC1, 1), new KafkaTopicModel(CLUSTER2_ID, 0, TOPIC2, 1));
    CompletableFuture<TopicQueryResults> expectedFuture = new CompletableFuture();
    expectedFuture.complete(new TopicQueryResults(expectedClustersFromMario, expectedTopicsFromMario));

    when(_marioClient.queryTopics(argThat(new TopicQueryMatcher(expectedQuery)))).thenReturn(expectedFuture);

    TopicPartition topicPartition1 = new TopicPartition(TOPIC1, 0);
    TopicPartition topicPartition2 = new TopicPartition(TOPIC2, 0);
    Map<TopicPartition, ClusterDescriptor> expectedResult = new HashMap<>();
    expectedResult.put(topicPartition1, CLUSTER1);
    expectedResult.put(topicPartition2, CLUSTER2);
    assertEquals(expectedResult, _marioMetadataServiceClient.getClustersForTopicPartitions(_clientId,
        Arrays.asList(topicPartition1, topicPartition2), 1000));
  }
}