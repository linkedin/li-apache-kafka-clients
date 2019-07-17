/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import java.util.Collections;
import java.util.Map;
import java.util.Set;


// This contains the result of the location lookup for a set of topics across multiple clusters in a cluster group.
public class TopicLookupResult extends LocationLookupResult {
  private Map<ClusterDescriptor, Set<String>> _topicsByCluster;

  public TopicLookupResult() {
    this(Collections.emptyMap(), Collections.emptySet());
  }

  public TopicLookupResult(Map<ClusterDescriptor, Set<String>> topicsByCluster, Set<String> nonexistentTopics) {
    super(nonexistentTopics);
    _topicsByCluster = topicsByCluster;
  }

  @Override
  public LocationLookupResult.ValueType getValueType() {
    return LocationLookupResult.ValueType.TOPICS;
  }

  public Map<ClusterDescriptor, Set<String>> getTopicsByCluster() {
    return _topicsByCluster;
  }
}