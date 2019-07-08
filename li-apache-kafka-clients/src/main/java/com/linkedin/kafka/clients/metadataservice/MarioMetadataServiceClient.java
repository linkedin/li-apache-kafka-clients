/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.metadataservice;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.common.LiKafkaFederatedClient;
import com.linkedin.kafka.clients.common.PartitionLookupResult;
import com.linkedin.kafka.clients.common.TopicLookupResult;
import com.linkedin.mario.client.MarioClient;
import com.linkedin.mario.client.models.v1.TopicQuery;
import com.linkedin.mario.client.util.MarioClusterGroupDescriptor;
import com.linkedin.mario.common.models.v1.KafkaClusterDescriptor;
import com.linkedin.mario.common.models.v1.KafkaTopicModel;
import com.linkedin.mario.common.models.v1.TopicQueryResults;
import com.linkedin.mario.common.websockets.MarioCommandCallback;

import com.linkedin.mario.common.websockets.Messages;
import com.linkedin.mario.common.websockets.MsgType;
import com.linkedin.mario.common.websockets.ReloadConfigResponseMessages;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.TopicPartition;


// This is a client for Mario metadata service.
public class MarioMetadataServiceClient implements MetadataServiceClient {
  private final MarioClient _marioClient;

  public MarioMetadataServiceClient(String serviceURI) {
    this(new MarioClient(serviceURI, false));
  }

  public MarioMetadataServiceClient(MarioClient marioClient) {
    _marioClient = marioClient;
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public void registerFederatedClient(LiKafkaFederatedClient federatedClient, ClusterGroupDescriptor clusterGroup,
      Map<String, ?> configs, int timeoutMs) {
    if (clusterGroup == null) {
      throw new IllegalArgumentException("cluster group cannot be null");
    }

    MarioClusterGroupDescriptor marioClusterGroup = new MarioClusterGroupDescriptor(clusterGroup.getName(),
        clusterGroup.getEnvironment());
    MarioCommandCallback marioCommandCallback = new MarioCommandCallbackImpl(federatedClient);
    _marioClient.registerFederatedClient(marioClusterGroup, (Map<String, String>) configs, timeoutMs, marioCommandCallback);
  }

  @Override
  public ClusterDescriptor getClusterForTopic(String topicName, ClusterGroupDescriptor clusterGroup, int timeoutMs)
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
  public PartitionLookupResult getClustersForTopicPartitions(
      Set<TopicPartition> topicPartitions, ClusterGroupDescriptor clusterGroup, int timeoutMs)
      throws MetadataServiceClientException {
    if (clusterGroup == null) {
      throw new IllegalArgumentException("cluster group cannot be null");
    }

    // Create a set of topic names out of topicPartitions.
    Set<String> topics = new HashSet<>();
    for (TopicPartition topicPartition : topicPartitions) {
      String topic = topicPartition.topic();
      if (topic == null || topic.trim().isEmpty()) {
        throw new IllegalArgumentException("topic cannot be null or empty");
      }
      topics.add(topic);
    }

    // TODO: When Mario serves partition counts, check for out-of-range partitions and return them separately.
    Map<String, Set<ClusterDescriptor>> topicToClusterMap = getClusterMapForTopics(topics, clusterGroup, timeoutMs);
    Map<ClusterDescriptor, Set<TopicPartition>> partitionsByCluster = new HashMap<>();
    Set<String> nonexistentTopics = new HashSet<>();
    for (TopicPartition topicPartition : topicPartitions) {
      String topic = topicPartition.topic();
      Set<ClusterDescriptor> clusters = topicToClusterMap.get(topic);
      if (clusters == null || clusters.isEmpty()) {
        nonexistentTopics.add(topic);
        continue;
      }

      // Unless topic move is in progress, a topic must exist in only one cluster.
      if (clusters.size() > 1) {
        throw new IllegalStateException("topic " + topic + " exists in more than one cluster " + clusters +
            " in cluster group " + clusterGroup);
      }

      partitionsByCluster.computeIfAbsent(clusters.iterator().next(), k -> new HashSet<TopicPartition>())
          .add(topicPartition);
    }
    return new PartitionLookupResult(partitionsByCluster, nonexistentTopics);
  }

  @Override
  public TopicLookupResult getClustersForTopics(Set<String> topics, ClusterGroupDescriptor clusterGroup,
      int timeoutMs) throws MetadataServiceClientException {
    if (clusterGroup == null) {
      throw new IllegalArgumentException("cluster group cannot be null");
    }

    for (String topic : topics) {
      if (topic == null || topic.trim().isEmpty()) {
        throw new IllegalArgumentException("topic cannot be null or empty");
      }
    }

    Map<String, Set<ClusterDescriptor>> clustersByTopic = getClusterMapForTopics(topics, clusterGroup, timeoutMs);
    Map<ClusterDescriptor, Set<String>> topicsByCluster = new HashMap<>();
    Set<String> nonexistentTopics = new HashSet<>();
    for (String topic : topics) {
      Set<ClusterDescriptor> clusters = clustersByTopic.get(topic);
      if (clusters == null || clusters.isEmpty()) {
        nonexistentTopics.add(topic);
        continue;
      }

      // Unless topic move is in progress, a topic must exist in only one cluster.
      if (clusters.size() > 1) {
        throw new IllegalStateException("topic " + topic + " exists in more than one cluster " + clusters +
            " in cluster group " + clusterGroup);
      }

      topicsByCluster.computeIfAbsent(clusters.iterator().next(), k -> new HashSet<String>()).add(topic);
    }
    return new TopicLookupResult(topicsByCluster, nonexistentTopics);
  }

  // For given topic names, construct a map from topic name to cluster descriptors where the topic exists. If a topic
  // does not exist, there will be no entry for that topic in the return map.
  private Map<String, Set<ClusterDescriptor>> getClusterMapForTopics(Set<String> topicNames,
      ClusterGroupDescriptor clusterGroup, int timeoutMs) throws MetadataServiceClientException {
    TopicQuery query = new TopicQuery(true, null, new HashSet<>(Arrays.asList(clusterGroup.getName())),
        null, new HashSet<>(Arrays.asList(clusterGroup.getEnvironment())), topicNames);
    TopicQueryResults queryResult = null;
    try {
      queryResult = _marioClient.queryTopics(query).get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new MetadataServiceClientException("topic query to mario failed with timeout " + timeoutMs + " " +
          TimeUnit.MILLISECONDS + ": ", e);
    } catch (Exception e) {
      throw new MetadataServiceClientException("topic query to mario failed: ", e);
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
  public void reportCommandExecutionComplete(UUID commandId, Map<String, String> configs, MsgType messageType) {
    Messages messageToSent;
    switch (messageType) {
      case RELOAD_CONFIG_RESPONSE:
        messageToSent = new ReloadConfigResponseMessages(configs, commandId);
        break;
      default:
        throw new UnsupportedOperationException("Message type " + messageType + " is not supported right now");
    }

    _marioClient.reportCommandExecutionComplete(commandId, messageToSent);
  }

  @Override
  public void reRegisterFederatedClient(Map<String, String> configs) {
    _marioClient.reRegisterFederatedClient(configs);
  }

  @Override
  public void close(int timeoutMs) {
  }
}