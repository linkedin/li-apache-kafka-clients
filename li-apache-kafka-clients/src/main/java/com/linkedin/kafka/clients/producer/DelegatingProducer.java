/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import org.apache.kafka.clients.producer.Producer;


public interface DelegatingProducer<K, V> extends Producer<K, V> {
  Producer<K, V> getDelegate();
}
