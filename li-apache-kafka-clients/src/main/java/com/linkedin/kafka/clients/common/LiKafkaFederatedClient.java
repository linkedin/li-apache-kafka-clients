/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;


// A superinterface for LiKafkaFederatedProducerImpl and LiKafkaFederatedConsumerImpl.
public interface LiKafkaFederatedClient {
  LiKafkaFederatedClientType getClientType();
}