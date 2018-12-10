/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.annotations.InterfaceOrigin;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.clients.producer.Producer;


/**
 * The general producer interface that allows allows pluggable serializers and deserializers.
 * LiKafkaProducer has the same interface as open source {@link Producer}. We define the interface separately to allow
 * future extensions.
 * @see LiKafkaProducerImpl
 */
public interface LiKafkaProducer<K, V> extends Producer<K, V> {
  /**
   * Flush any accumulated records from the producer. If the close does not complete within the timeout, throws exception.
   * If the underlying producer does not support bounded flush, this method defaults to {@link #flush()}
   * TODO: This API is added as a HOTFIX until the API change is available in apache/kafka
   */
  @InterfaceOrigin.LiKafkaClients
  void flush(long timeout, TimeUnit timeUnit);

  @InterfaceOrigin.LiKafkaClients
  Map<String, List<PartitionInfo>> partitionsFor(Set<String> topics);
}
