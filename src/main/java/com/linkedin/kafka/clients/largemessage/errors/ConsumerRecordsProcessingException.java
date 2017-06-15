package com.linkedin.kafka.clients.largemessage.errors;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class ConsumerRecordsProcessingException extends RuntimeException {
  private final List<RecordProcessingException> _recordProcessingExceptions;

  public ConsumerRecordsProcessingException(List<RecordProcessingException> exceptions) {
    super(String.format("Received exception when processing messages for %d partitions.", exceptions.size()), exceptions.get(0));
    Iterator<RecordProcessingException> exceptionIterator = exceptions.iterator();
    // skip the first exception.
    exceptionIterator.next();
    while (exceptionIterator.hasNext()) {
      addSuppressed(exceptionIterator.next());
    }
    _recordProcessingExceptions = exceptions;
  }

  public List<RecordProcessingException> recordProcessingExceptions() {
    return Collections.unmodifiableList(_recordProcessingExceptions);
  }
}
