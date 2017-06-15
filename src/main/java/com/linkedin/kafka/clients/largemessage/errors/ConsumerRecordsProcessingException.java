package com.linkedin.kafka.clients.largemessage.errors;

import java.util.Collections;
import java.util.List;


public class ConsumerRecordsProcessingException extends RuntimeException {
  private final List<RecordProcessingException> _recordProcessingExceptions;

  public ConsumerRecordsProcessingException(List<RecordProcessingException> exceptions) {
    super(String.format("Received exception when processing messages for %d partitions.", exceptions.size()));
    _recordProcessingExceptions = exceptions;
  }

  public List<RecordProcessingException> recordProcessingExceptions() {
    return Collections.unmodifiableList(_recordProcessingExceptions);
  }
}
