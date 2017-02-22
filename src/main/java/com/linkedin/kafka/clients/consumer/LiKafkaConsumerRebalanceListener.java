/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * The rebalance listener for LiKafkaClients that is large message aware.
 */
class LiKafkaConsumerRebalanceListener<K, V> implements ConsumerRebalanceListener {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaConsumerRebalanceListener.class);
  private final ConsumerRecordsProcessor _consumerRecordsProcessor;
  private final LiKafkaConsumer _consumer;
  private final Set<TopicPartition> _partitionsRemoved;
  private final boolean _autoCommitEnabled;
  private ConsumerRebalanceListener _userListener;

  LiKafkaConsumerRebalanceListener(ConsumerRecordsProcessor consumerRecordsProcessor,
                                   LiKafkaConsumer consumer,
                                   boolean autoCommitEnabled) {
    _consumerRecordsProcessor = consumerRecordsProcessor;
    _consumer = consumer;
    _partitionsRemoved = new HashSet<>();
    _autoCommitEnabled = autoCommitEnabled;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
    LOG.debug("Consumer rebalancing. Revoked partitions: {}", topicPartitions);
    // Record the partitions that might be revoked, if the partitions are really revoked, we need to clean up
    // the state.
    _partitionsRemoved.clear();
    _consumerRecordsProcessor.clearAllConsumerHighWaterMarks();
    _partitionsRemoved.addAll(topicPartitions);
    // Fire user listener.
    _userListener.onPartitionsRevoked(topicPartitions);
    // Commit offset if auto commit is enabled.
    if (_autoCommitEnabled) {
      _consumer.commitSync();
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
    LOG.debug("Consumer rebalancing. Assigned partitions: {}", topicPartitions);
    // Remove the partitions that are assigned back to this consumer
    _partitionsRemoved.removeAll(topicPartitions);
    for (TopicPartition tp : _partitionsRemoved) {
      _consumerRecordsProcessor.clear(tp);
    }
    // Fire user listener.
    _userListener.onPartitionsAssigned(topicPartitions);
  }

  public void setUserListener(ConsumerRebalanceListener userListener) {
    _userListener = userListener;
  }
}
