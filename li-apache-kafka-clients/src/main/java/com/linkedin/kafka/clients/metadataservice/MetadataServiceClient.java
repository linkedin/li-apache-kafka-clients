/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.metadataservice;

import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;

import java.io.Closeable;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.Configurable;


// An interface to a generic metadata service which serves cluster and topic metadata for federated Kafka clusters.
public interface MetadataServiceClient extends Configurable, Closeable {
  /**
   * Register a federated client with the metadata service. Called by the client to be registered.
   *
   * @param clusterGroup  The cluster group descriptor
   * @param configs  Client configs
   * @return The id of the registered client
   */
  public UUID registerFederatedClient(ClusterGroupDescriptor clusterGroup, Map<String, ?> configs);

  /**
   * Get the cluster name for the given topic from the given cluster group.
   *
   * @param clientId          The id of the client
   * @param topicName         The topic name
   * @return The name of the physical cluster where the topic is hosted
   */
  public String getClusterForTopic(UUID clientId, String topicName);
}