/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.Consumer;


public interface DelegatingConsumer<K, V> extends Consumer<K, V> {
  Consumer<K, V> getDelegate();
}
