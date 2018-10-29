/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

/**
 * Since enum are essentially final in Java, we are duplicating the values from
 * the open-source equivalent {@link org.apache.kafka.clients.consumer.OffsetResetStrategy}
 *
 * This workaround needs to be provided until apache/kafka can support a closest reset policy,
 * aka KIP-320 (https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation)
 */
public enum LiOffsetResetStrategy {
  EARLIEST, LATEST, NONE, LICLOSEST
}
