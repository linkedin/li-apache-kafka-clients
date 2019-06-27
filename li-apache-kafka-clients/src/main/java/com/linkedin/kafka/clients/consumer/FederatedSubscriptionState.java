/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;

// Current subscription/assignment state at the federated level
public class FederatedSubscriptionState {
  public enum SubscriptionType {
    NONE, MANUAL_ASSIGNMENT, SUBSCRIPTION, PATTERN_SUBSCRIPTION
  }

  private SubscriptionType _subscriptionType;

  // Topics that are part of the current (non-pattern) subscription/assignment but do not exist yet
  private Set<String> _topicsWaitingToBeCreated;

  public FederatedSubscriptionState(SubscriptionType subscriptionType) {
    this(subscriptionType, Collections.emptySet());
  }

  public FederatedSubscriptionState(SubscriptionType subscriptionType, Set<String> topicsWaitingToBeCreated) {
    _subscriptionType = subscriptionType;
    _topicsWaitingToBeCreated = topicsWaitingToBeCreated;
  }

  public SubscriptionType getSubscriptionType() {
    return _subscriptionType;
  }

  public void setTopicsWaitingToBeCreated(Set<String> topicsWaitingToBeCreated) {
    _topicsWaitingToBeCreated = topicsWaitingToBeCreated;
  }

  public Set<String> getTopicsWaitingToBeCreated() {
    return Collections.unmodifiableSet(new HashSet<>(_topicsWaitingToBeCreated));
  }
}

class Unsubscribed extends FederatedSubscriptionState {
  public Unsubscribed() {
    super(SubscriptionType.NONE);
  }
}

class ManuallyAssigned extends FederatedSubscriptionState {
  private Set<TopicPartition> _assignment;

  public ManuallyAssigned(Set<TopicPartition> assignment, Set<String> topicsWaitingToBeCreated) {
    super(SubscriptionType.MANUAL_ASSIGNMENT, topicsWaitingToBeCreated);
    _assignment = assignment;
  }

  public Set<TopicPartition> getAssignment() {
    return Collections.unmodifiableSet(new HashSet<>(_assignment));
  }
}

class Subscribed extends FederatedSubscriptionState {
  private Set<String> _subscription;

  public Subscribed(Set<String> subscription, Set<String> topicsWaitingToBeCreated) {
    super(SubscriptionType.SUBSCRIPTION, topicsWaitingToBeCreated);
    _subscription = subscription;
  }

  public Set<String> getSubscription() {
    return Collections.unmodifiableSet(new HashSet<>(_subscription));
  }
}

class PatternSubscribed extends FederatedSubscriptionState {
  private Pattern _subscribedPattern;

  public PatternSubscribed(Pattern subscribedPattern) {
    super(SubscriptionType.PATTERN_SUBSCRIPTION);
    _subscribedPattern = subscribedPattern;
  }

  public Pattern getSubscribedPattern() {
    return _subscribedPattern;
  }
}
