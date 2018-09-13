/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage.errors;

import org.apache.kafka.common.TopicPartition;


/**
 * Thrown when an offset is not tracked by the consumer. LiKafkaConsumerImpl keeps track of all the messages it has
 * consumed to support large message aware seek(). This exception indicates that the users is trying to seek back
 * to an offset that is less than the earliest offset the consumer has ever consumed.
 */
public class OffsetNotTrackedException extends LargeMessageException {
  private final TopicPartition _topicPartition;
  private final long _offset;

  public OffsetNotTrackedException(TopicPartition topicPartition, long offset, String message) {
    super(message);
    _topicPartition = topicPartition;
    _offset = offset;
  }

  public TopicPartition getTopicPartition() {
    return _topicPartition;
  }

  public long getOffset() {
    return _offset;
  }
}
