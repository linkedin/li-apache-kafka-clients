/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.metadataservice;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.common.FederatedClientCommandCallback;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;


// An interface to a generic metadata service which serves cluster and topic metadata for federated Kafka clusters.
public interface MetadataServiceClient extends Configurable, AutoCloseable {
  /**
   * Register a federated client with the metadata service. Called by the client to be registered.
   *
   * @param clusterGroup  The cluster group descriptor
   * @param configs       Client configs
   * @param callbacks     A collection of callbacks supported by the client which will be called upon receiving command
   *                      execution requests from the metadata service
   * @param timeoutMs     Timeout in milliseconds
   */
  public void registerFederatedClient(ClusterGroupDescriptor clusterGroup, Map<String, ?> configs,
      Collection<FederatedClientCommandCallback> callbacks, int timeoutMs);

  /**
   * Get the cluster name for the given topic. If the topic does not exist in this group, return null.
   *
   * @param topicName  The topic name
   * @param clusterGroup  The cluster group descriptor
   * @param timeoutMs  Timeout in milliseconds
   * @return The descriptor of the physical cluster where the topic is hosted
   */
  public ClusterDescriptor getClusterForTopic(String topicName, ClusterGroupDescriptor clusterGroup, int timeoutMs)
      throws MetadataServiceClientException;

  /**
   * Get a map from the given topic partitions to the clusters where they are hosted. For nonexistent topic partitions,
   * the cluster will be set to null.
   *
   * @param topicPartitions  The topic partitions
   * @param clusterGroup  The cluster group descriptor
   * @param timeoutMs        Timeout in milliseconds
   * @return A map from topic partitions to the descriptors of the physical clusters where they are hosted
   */
  public Map<TopicPartition, ClusterDescriptor> getClustersForTopicPartitions(
      Collection<TopicPartition> topicPartitions, ClusterGroupDescriptor clusterGroup, int timeoutMs)
      throws MetadataServiceClientException;

  /**
   * Close this metadata service client with the specified timeout.
   *
   * @param timeoutMs  Timeout in milliseconds
   */
  public void close(int timeoutMs);

  /**
   * Close this metadata service client with the maximum timeout.
   */
  default public void close() {
    close(Integer.MAX_VALUE);
  }
}