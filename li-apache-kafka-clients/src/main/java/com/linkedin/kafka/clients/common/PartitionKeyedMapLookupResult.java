/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;


// This contains the result of the location lookup for a map keyed by partition across multiple clusters in a cluster
// group.
public class PartitionKeyedMapLookupResult<T> extends LocationLookupResult {
  private Map<ClusterDescriptor, Map<TopicPartition, T>> _partitionKeyedMapsByCluster;

  public PartitionKeyedMapLookupResult() {
    this(Collections.emptyMap(), Collections.emptySet());
  }

  public PartitionKeyedMapLookupResult(Map<ClusterDescriptor, Map<TopicPartition, T>> partitionKeyedMapsByCluster,
      Set<String> nonexistentTopics) {
    super(nonexistentTopics);
    _partitionKeyedMapsByCluster = partitionKeyedMapsByCluster;
  }

  @Override
  public LocationLookupResult.ValueType getValueType() {
    return LocationLookupResult.ValueType.PARTITION_KEYED_MAP;
  }

  public Map<ClusterDescriptor, Map<TopicPartition, T>> getPartitionKeyedMapsByCluster() {
    return _partitionKeyedMapsByCluster;
  }
}