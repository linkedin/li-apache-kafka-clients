/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import java.util.Collections;
import java.util.Set;


// This contains the result of the location lookup for a set of topics/partitions or a map of values keyed by
// topic/partitition.
public abstract class LocationLookupResult {
  public enum ValueType {
    PARTITIONS, TOPICS, PARTITION_KEYED_MAP
  }

  private Set<String> _nonexistentTopics;

  public LocationLookupResult() {
    _nonexistentTopics = Collections.emptySet();
  }

  public LocationLookupResult(/*Map<ClusterDescriptor, T> valuesByCluster, */Set<String> nonexistentTopics) {
    _nonexistentTopics = nonexistentTopics;
  }

  public abstract ValueType getValueType();

  public Set<String> getNonexistentTopics() {
    return _nonexistentTopics;
  }
}