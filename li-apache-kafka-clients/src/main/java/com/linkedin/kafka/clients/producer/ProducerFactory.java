/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;


@FunctionalInterface
public interface ProducerFactory<K, V> {
  Producer<K, V> create(Properties base, Properties overrides);
}