/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import java.util.Collections;
import java.util.Map;
import java.util.Set;


// This contains the result of the location lookup for a set of topics across multiple clusters in a cluster group.
public class TopicLookupResult {
  private Map<ClusterDescriptor, Set<String>> _topicsByCluster;
  private Set<String> _nonexistentTopics;

  public TopicLookupResult() {
    _topicsByCluster = Collections.emptyMap();
    _nonexistentTopics = Collections.emptySet();
  }

  public TopicLookupResult(Map<ClusterDescriptor, Set<String>> topicsByCluster, Set<String> nonexistentTopics) {
    _topicsByCluster = topicsByCluster;
    _nonexistentTopics = nonexistentTopics;
  }

  public Map<ClusterDescriptor, Set<String>> getTopicsByCluster() {
    return _topicsByCluster;
  }

  public Set<String> getNonexistentTopics() {
    return _nonexistentTopics;
  }
}