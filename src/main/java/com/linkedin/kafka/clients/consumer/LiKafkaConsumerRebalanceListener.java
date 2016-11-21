/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.largemessage.ConsumerRecordsProcessor;
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
