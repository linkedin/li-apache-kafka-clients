/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.metadataservice;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.common.LiKafkaFederatedClient;
import com.linkedin.kafka.clients.common.PartitionLookupResult;
import com.linkedin.kafka.clients.common.TopicLookupResult;

import com.linkedin.mario.common.websockets.MessageType;
import java.util.Map;
import java.util.Set;

import java.util.UUID;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;


// An interface to a generic metadata service which serves cluster and topic metadata for federated Kafka clusters.
public interface MetadataServiceClient extends Configurable, AutoCloseable {
  /**
   * Register a federated client with the metadata service. Called by the client to be registered.
   *
   * @param federatedClient The client being registered
   * @param clusterGroup    The cluster group descriptor
   * @param configs         Client configs
   * @param timeoutMs       Timeout in milliseconds
   */
  void registerFederatedClient(LiKafkaFederatedClient federatedClient, ClusterGroupDescriptor clusterGroup,
      Map<String, ?> configs, int timeoutMs);

  /**
   * Get the cluster name for the given topic. If the topic does not exist in this group, return null.
   *
   * @param topicName  The topic name
   * @param clusterGroup  The cluster group descriptor
   * @param timeoutMs  Timeout in milliseconds
   * @return The descriptor of the physical cluster where the topic is hosted
   */
  ClusterDescriptor getClusterForTopic(String topicName, ClusterGroupDescriptor clusterGroup, int timeoutMs)
      throws MetadataServiceClientException;

  /**
   * Get the clusters which host the given topic partitions. The result will also contain any nonexistent topics.
   *
   * @param topicPartitions  The topic partitions
   * @param clusterGroup  The cluster group descriptor
   * @param timeoutMs        Timeout in milliseconds
   * @return Location lookup result
   */
  PartitionLookupResult getClustersForTopicPartitions(Set<TopicPartition> topicPartitions,
      ClusterGroupDescriptor clusterGroup, int timeoutMs) throws MetadataServiceClientException;

  /**
   * Get the clusters which host the given topics. The result will also contain any nonexistent topics.
   *
   * @param topics  The topics
   * @param clusterGroup  The cluster group descriptor
   * @param timeoutMs        Timeout in milliseconds
   * @return Location lookup result
   */
  TopicLookupResult getClustersForTopics(Set<String> topics, ClusterGroupDescriptor clusterGroup,
      int timeoutMs) throws MetadataServiceClientException;

  /**
   * Report to mario server that command execution for commandId is completed
   * @param commandId                UUID identifying the completed command
   * @param configs                  config diff before and after the command
   * @param messageType              response message type to mario server
   * @param commandExecutionResult   execution result of the given command
   */
  void reportCommandExecutionComplete(UUID commandId, Map<String, String> configs, MessageType messageType, boolean commandExecutionResult);

  /**
   * Re-register federated client with new set of configs
   * @param configs  config diff from original for current federated client
   */
  void reRegisterFederatedClient(Map<String, String> configs);

  /**
   * Close this metadata service client with the specified timeout.
   *
   * @param timeoutMs  Timeout in milliseconds
   */
  void close(int timeoutMs);

  /**
   * Close this metadata service client with the maximum timeout.
   */
  default void close() {
    close(Integer.MAX_VALUE);
  }
}