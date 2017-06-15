package com.linkedin.kafka.clients.largemessage.errors;

import org.apache.kafka.common.TopicPartition;


/**
 * An exception indicating a consumer record processing encountered error.
 */
public class RecordProcessingException extends RuntimeException {
  private final TopicPartition _topicPartition;
  private final long _offset;

  public RecordProcessingException(TopicPartition tp, long offset, Throwable cause) {
    super(cause);
    _topicPartition = tp;
    _offset = offset;
  }

  public TopicPartition topicPartition() {
    return _topicPartition;
  }

  public long offset() {
    return _offset;
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }
}
