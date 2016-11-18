/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.producer.ExtensibleProducerRecord;
import java.util.Collection;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.UUID;

/**
 * Message splitter for large messages
 *
 * TODO: just make this an interceptor interface?
 */
public interface MessageSplitter<K, V> {

  /**
   * Split the large message into several {@link com.linkedin.kafka.clients.producer.ExtensibleProducerRecord}
   *
   * @param previousRecord The key and value contains the results of serializing the original message.
   * @return A collection of records each contains a chunk of the original records' value.  Each emitted record must
   * have all the same header records as previousRecord.  If the message is small enough (as decided by MessageSplitter)
   * then this may return a singleton collection with just previousRecord.
   */
  Collection<ExtensibleProducerRecord<byte[], byte[]>> split(ExtensibleProducerRecord<byte[], byte[]> previousRecord);



}

