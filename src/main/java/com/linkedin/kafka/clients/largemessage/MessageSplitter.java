/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.producer.ExtensibleProducerRecord;
import java.util.Collection;

/**
 * Message splitter for large messages
 */
public interface MessageSplitter<K, V> {

  /**
   * Split the large message into several {@link com.linkedin.kafka.clients.producer.ExtensibleProducerRecord}
   *
   * @param originalRecord The key and value contains the results of serializing the original message.
   * @return A collection of records each contains a chunk of the original records' value.  Each emitted record must
   * have all the same header records as previousRecord.  If the message is small enough (as decided by MessageSplitter)
   * then this may return a singleton collection with just previousRecord.
   */
  Collection<ExtensibleProducerRecord<byte[], byte[]>> split(ExtensibleProducerRecord<byte[], byte[]> originalRecord);

}

