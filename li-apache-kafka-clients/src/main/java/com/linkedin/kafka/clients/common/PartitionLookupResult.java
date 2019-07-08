/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;


// This contains the result of the location lookup for a set of partitions across multiple clusters in a cluster group.
public class PartitionLookupResult {
  private Map<ClusterDescriptor, Set<TopicPartition>> _partitionsByCluster;
  private Set<String> _nonexistentTopics;

  public PartitionLookupResult() {
    _partitionsByCluster = Collections.emptyMap();
    _nonexistentTopics = Collections.emptySet();
  }

  public PartitionLookupResult(Map<ClusterDescriptor, Set<TopicPartition>> partitionsByCluster,
      Set<String> nonexistentTopics) {
    _partitionsByCluster = partitionsByCluster;
    _nonexistentTopics = nonexistentTopics;
  }

  public Map<ClusterDescriptor, Set<TopicPartition>> getPartitionsByCluster() {
    return _partitionsByCluster;
  }

  public Set<String> getNonexistentTopics() {
    return _nonexistentTopics;
  }
}