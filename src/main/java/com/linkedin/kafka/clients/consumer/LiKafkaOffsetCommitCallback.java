/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
