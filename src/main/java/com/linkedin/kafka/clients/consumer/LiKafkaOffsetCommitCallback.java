/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

/**
 * The offset commit callback for LiKafkaConsumer which is large message aware.
 */
class LiKafkaOffsetCommitCallback implements OffsetCommitCallback {
  private OffsetCommitCallback _userCallback = null;

  @Override
  public void onComplete(Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap, Exception e) {
    if (_userCallback != null) {
      Map<TopicPartition, OffsetAndMetadata> userOffsetMap = topicPartitionOffsetAndMetadataMap;
      if (topicPartitionOffsetAndMetadataMap != null) {
        userOffsetMap = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
          String rawMetadata = entry.getValue().metadata();
          long userOffset = LiKafkaClientsUtils.offsetFromWrappedMetadata(rawMetadata);
          String userMetadata = LiKafkaClientsUtils.metadataFromWrappedMetadata(rawMetadata);
          userOffsetMap.put(entry.getKey(), new OffsetAndMetadata(userOffset, userMetadata));
        }
      }
      _userCallback.onComplete(userOffsetMap, e);
    }
  }

  public void setUserCallback(OffsetCommitCallback userCallback) {
    _userCallback = userCallback;
  }
}
