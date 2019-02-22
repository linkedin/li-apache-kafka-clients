/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.metadataservice;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;

import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.Configurable;


// An interface to a generic metadata service which serves cluster and topic metadata for federated Kafka clusters.
public interface MetadataServiceClient extends Configurable, AutoCloseable {
  /**
   * Register a federated client with the metadata service. Called by the client to be registered.
   *
   * @param clusterGroup  The cluster group descriptor
   * @param configs       Client configs
   * @param timeoutMs     Timeout in milliseconds
   * @return The id of the registered client
   */
  public UUID registerFederatedClient(ClusterGroupDescriptor clusterGroup, Map<String, ?> configs, int timeoutMs);

  /**
   * Get the cluster name for the given topic from the given cluster group.
   *
   * @param clientId   The id of the client
   * @param topicName  The topic name
   * @param timeoutMs     Timeout in milliseconds
   * @return The descriptor of the physical cluster where the topic is hosted
   */
  public ClusterDescriptor getClusterForTopic(UUID clientId, String topicName, int timeoutMs);

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