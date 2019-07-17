/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;


// This contains the result of the location lookup for a set of partitions across multiple clusters in a cluster group.
public class PartitionLookupResult extends LocationLookupResult {
  private Map<ClusterDescriptor, Set<TopicPartition>> _partitionsByCluster;

  public PartitionLookupResult() {
    this(Collections.emptyMap(), Collections.emptySet());
  }

  public PartitionLookupResult(Map<ClusterDescriptor, Set<TopicPartition>> partitionsByCluster,
      Set<String> nonexistentTopics) {
    super(nonexistentTopics);
    _partitionsByCluster = partitionsByCluster;
  }

  @Override
  public LocationLookupResult.ValueType getValueType() {
    return LocationLookupResult.ValueType.PARTITIONS;
  }

  public Map<ClusterDescriptor, Set<TopicPartition>> getPartitionsByCluster() {
    return _partitionsByCluster;
  }
}