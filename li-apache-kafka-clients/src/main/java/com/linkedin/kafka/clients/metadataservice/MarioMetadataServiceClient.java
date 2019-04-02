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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.TopicPartition;


// This is a client for Mario metadata service.
public class MarioMetadataServiceClient implements MetadataServiceClient {
  private static final int DEFAULT_MAX_RETRIES = 3;

  private final MarioClient _marioClient;
  private final int _maxRetries;

  public MarioMetadataServiceClient(String serviceURI) {
    this(new MarioClient(serviceURI));
  }

  public MarioMetadataServiceClient(MarioClient marioClient) {
    this(marioClient, DEFAULT_MAX_RETRIES);
  }

  public MarioMetadataServiceClient(MarioClient marioClient, int maxRetries) {
    _maxRetries = maxRetries;
    _marioClient = marioClient;
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public UUID registerFederatedClient(ClusterGroupDescriptor clusterGroup, Map<String, ?> configs, int timeoutMs) {
    if (clusterGroup == null) {
      throw new IllegalArgumentException("cluster group cannot be null");
    }

    return UUID.randomUUID();
  }

  @Override
  public ClusterDescriptor getClusterForTopic(UUID clientId, String topicName, ClusterGroupDescriptor clusterGroup,
      int timeoutMs)
      throws MetadataServiceClientException {
    if (clusterGroup == null) {
      throw new IllegalArgumentException("cluster group cannot be null");
    }

    Set<ClusterDescriptor> clusters = getClusterMapForTopics(new HashSet<>(Arrays.asList(topicName)), clusterGroup,
        timeoutMs).get(topicName);
    if (clusters == null || clusters.isEmpty()) {
      return null;
    }
    // Unless topic move is in progress, a topic must exist in only one cluster.
    if (clusters.size() > 1) {
      throw new IllegalStateException("topic " + topicName + " exists in more than one cluster " + clusters +
          " in cluster group " + clusterGroup);
    }
    return clusters.iterator().next();
  }

  @Override
  public Map<TopicPartition, ClusterDescriptor> getClustersForTopicPartitions(UUID clientId,
      Collection<TopicPartition> topicPartitions, ClusterGroupDescriptor clusterGroup, int timeoutMs)
      throws MetadataServiceClientException {
    if (clusterGroup == null) {
      throw new IllegalArgumentException("cluster group cannot be null");
    }

    // Create a set of topic names out of topicPartitions.
    Set<String> topicNames = new HashSet<>();
    for (Iterator<TopicPartition> it = topicPartitions.iterator(); it.hasNext(); ) {
      topicNames.add(it.next().topic());
    }

    Map<String, Set<ClusterDescriptor>> topicToClusterMap = getClusterMapForTopics(topicNames, clusterGroup, timeoutMs);
    Map<TopicPartition, ClusterDescriptor> topicPartitionToClusterMap = new HashMap<>();
    for (Iterator<TopicPartition> it = topicPartitions.iterator(); it.hasNext(); ) {
      TopicPartition topicPartition = it.next();
      Set<ClusterDescriptor> clusters = topicToClusterMap.get(topicPartition.topic());
      if (clusters == null || clusters.isEmpty()) {
        continue;
      }

      // Unless topic move is in progress, a topic must exist in only one cluster.
      if (clusters.size() > 1) {
        throw new IllegalStateException("topic " + topicPartition.topic() + " exists in more than one cluster " +
            clusters + " in cluster group " + clusterGroup);
      }

      topicPartitionToClusterMap.put(topicPartition, clusters.iterator().next());
    }
    return topicPartitionToClusterMap;
  }

  // For given topic names, construct a map from topic getName to cluster descriptors where the topic exists. If a topic
  // does not exist, there will be no entry for that topic in the return map.
  private Map<String, Set<ClusterDescriptor>> getClusterMapForTopics(Set<String> topicNames,
      ClusterGroupDescriptor clusterGroup, int timeoutMs) throws MetadataServiceClientException {
    TopicQuery query = new TopicQuery(true, null, new HashSet<>(Arrays.asList(clusterGroup.getName())),
        null, new HashSet<>(Arrays.asList(clusterGroup.getEnvironment())), topicNames);
    TopicQueryResults queryResult = null;
    for (int count = 1; count <= _maxRetries; count++) {
      try {
        queryResult = _marioClient.queryTopics(query).get(timeoutMs, TimeUnit.MILLISECONDS);
        break;
      } catch (TimeoutException e) {
        if (count == _maxRetries) {
          throw new MetadataServiceClientException("topic query to mario failed after retrying " + _maxRetries +
              "times with timeout " + timeoutMs + " " + TimeUnit.MILLISECONDS + ": ", e);
        }
      } catch (Exception e) {
        throw new MetadataServiceClientException("topic query to mario failed: ", e);
      }
    }

    HashMap<UUID, ClusterDescriptor> clusterIdToClusterMap = new HashMap<>();
    for (KafkaClusterDescriptor kafkaClusterDescriptor: queryResult.getClusters()) {
      clusterIdToClusterMap.put(kafkaClusterDescriptor.getId(), new ClusterDescriptor(kafkaClusterDescriptor.getName(),
          kafkaClusterDescriptor.getBootstrapUrl(), kafkaClusterDescriptor.getZkConnection()));
    }

    HashMap<String, Set<ClusterDescriptor>> topicToClusterMap = new HashMap<>();
    for (KafkaTopicModel topic: queryResult.getTopics()) {
      ClusterDescriptor cluster = clusterIdToClusterMap.get(topic.getClusterId());
      if (cluster == null) {
        throw new IllegalStateException("cluster with id " + topic.getClusterId() + " not found in query result");
      }
      topicToClusterMap.computeIfAbsent(topic.getName(), k -> new HashSet<>()).add(cluster);
    }
    return topicToClusterMap;
  }

  @Override
  public void close(int timeoutMs) {
  }
}