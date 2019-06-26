/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;

// Current subscription/assignment state at the federated level
class FederatedSubscriptionState {
  public enum SubscriptionType {
    NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
  }

  private SubscriptionType _subscriptionType;

  private Set<String> _subscription;

  private Pattern _subscribedPattern;

  private Set<TopicPartition> _assignment;

  // Topics that are part of the current (non-pattern) subscription/assignment but do not exist yet
  private Set<String> _topicsWaitingToBeCreated;

  public FederatedSubscriptionState() {
    _subscriptionType = SubscriptionType.NONE;
    _subscription = Collections.emptySet();
    _assignment = Collections.emptySet();
    _subscribedPattern = null;
    _topicsWaitingToBeCreated = Collections.emptySet();
  }

  public SubscriptionType getSubscriptionType() {
    return _subscriptionType;
  }

  public void setAssignment(Set<TopicPartition> assignment) {
    clear();
    _subscriptionType = SubscriptionType.USER_ASSIGNED;
    _assignment = assignment;
  }

  public Set<TopicPartition> getAssignment() {
    return _assignment;
  }

  public void setSubscription(Set<String> subscription) {
    clear();
    _subscriptionType = SubscriptionType.AUTO_TOPICS;
    _subscription = subscription;
  }

  public Set<String> getSubscription() {
    return _subscription;
  }

  public void setSubscribedPattern(Pattern subscribedPattern) {
    clear();
    _subscriptionType = SubscriptionType.AUTO_PATTERN;
    _subscribedPattern = subscribedPattern;
  }

  public void setTopicsWaitingToBeCreated(Set<String> topicsWaitingToBeCreated) {
    _topicsWaitingToBeCreated = topicsWaitingToBeCreated;
  }

  public Set<String> getTopicsWaitingToBeCreated() {
    return _topicsWaitingToBeCreated;
  }

  public void unsubscribe() {
    clear();
    _subscriptionType = SubscriptionType.NONE;
  }

  private void clear() {
    _subscription = Collections.emptySet();
    _assignment = Collections.emptySet();
    _subscribedPattern = null;
    _topicsWaitingToBeCreated = Collections.emptySet();
  }
}
