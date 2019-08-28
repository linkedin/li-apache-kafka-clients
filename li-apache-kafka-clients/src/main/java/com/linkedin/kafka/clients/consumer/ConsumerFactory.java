/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;


@FunctionalInterface
public interface ConsumerFactory<K, V> {
  Consumer<K, V> create(Properties base, Properties overrides);
}