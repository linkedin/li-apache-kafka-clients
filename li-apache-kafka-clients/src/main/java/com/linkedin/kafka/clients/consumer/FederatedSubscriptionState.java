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
public abstract class FederatedSubscriptionState {
  public enum SubscriptionType {
    NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
  }

  // Topics that are part of the current (non-pattern) subscription/assignment but do not exist yet
  private Set<String> _topicsWaitingToBeCreated;

  protected FederatedSubscriptionState() {
    this(Collections.emptySet());
  }

  protected FederatedSubscriptionState(Set<String> topicsWaitingToBeCreated) {
    _topicsWaitingToBeCreated = topicsWaitingToBeCreated;
  }

  public abstract SubscriptionType getSubscriptionType();

  public void setTopicsWaitingToBeCreated(Set<String> topicsWaitingToBeCreated) {
    _topicsWaitingToBeCreated = topicsWaitingToBeCreated;
  }

  public Set<String> getTopicsWaitingToBeCreated() {
    return Collections.unmodifiableSet(new HashSet<>(_topicsWaitingToBeCreated));
  }
}

class Unsubscribed extends FederatedSubscriptionState {
  private static final Unsubscribed SINGLETON_INSTANCE = new Unsubscribed();

  private Unsubscribed() {
  }

  public static Unsubscribed getInstance() {
    return SINGLETON_INSTANCE;
  }

  @Override
  public SubscriptionType getSubscriptionType() {
    return SubscriptionType.NONE;
  }

  @Override
  public void setTopicsWaitingToBeCreated(Set<String> topicsWaitingToBeCreated) {
    throw new UnsupportedOperationException("the set of topics to be created cannot be set for unsubscribed state");
  }
}

class UserAssigned extends FederatedSubscriptionState {
  private Set<TopicPartition> _assignment;

  public UserAssigned(Set<TopicPartition> assignment, Set<String> topicsWaitingToBeCreated) {
    super(topicsWaitingToBeCreated);
    _assignment = assignment;
  }

  @Override
  public SubscriptionType getSubscriptionType() {
    return SubscriptionType.USER_ASSIGNED;
  }

  public Set<TopicPartition> getAssignment() {
    return Collections.unmodifiableSet(new HashSet<>(_assignment));
  }
}

class Subscribed extends FederatedSubscriptionState {
  private Set<String> _subscription;

  public Subscribed(Set<String> subscription, Set<String> topicsWaitingToBeCreated) {
    super(topicsWaitingToBeCreated);
    _subscription = subscription;
  }

  @Override
  public SubscriptionType getSubscriptionType() {
    return SubscriptionType.AUTO_TOPICS;
  }

  public Set<String> getSubscription() {
    return Collections.unmodifiableSet(new HashSet<>(_subscription));
  }
}

class PatternSubscribed extends FederatedSubscriptionState {
  private Pattern _subscribedPattern;

  public PatternSubscribed(Pattern subscribedPattern) {
    super();
    _subscribedPattern = subscribedPattern;
  }

  @Override
  public SubscriptionType getSubscriptionType() {
    return SubscriptionType.AUTO_PATTERN;
  }

  public Pattern getSubscribedPattern() {
    return _subscribedPattern;
  }
}
