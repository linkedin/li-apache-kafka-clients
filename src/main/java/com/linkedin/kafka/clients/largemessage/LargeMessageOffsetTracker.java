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

import com.linkedin.kafka.clients.largemessage.errors.OffsetNotTrackedException;
import com.linkedin.kafka.clients.utils.QueuedMap;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The class keeps track of the safe offset for each partition.
 */
class LargeMessageOffsetTracker {

  private final Map<TopicPartition, QueuedMap<UUID, Long>> _offsetMap;

  LargeMessageOffsetTracker() {
    _offsetMap = new HashMap<>();
  }

  Map<TopicPartition, Long> safeOffsets() {
    if (_offsetMap.size() == 0) {
      return Collections.emptyMap();
    }

    Map<TopicPartition, Long> safeOffsetMap = new HashMap<>();
    for (TopicPartition tp : _offsetMap.keySet()) {
      QueuedMap<UUID, Long> offsetMapForPartition = _offsetMap.get(tp);
      UUID eldest = offsetMapForPartition.getEldestKey();
      if (eldest != null) {
        safeOffsetMap.put(tp, offsetMapForPartition.get(eldest));
      }
    }
    return safeOffsetMap;
  }

  long safeOffset(TopicPartition tp) {
    long offset = Long.MAX_VALUE;
    QueuedMap<UUID, Long> offsetMapForPartition = _offsetMap.get(tp);
    if (offsetMapForPartition != null) {
      UUID eldest = offsetMapForPartition.getEldestKey();
      if (eldest != null) {
        offset = offsetMapForPartition.get(eldest);
      }
    }
    return offset;
  }

  void untrackMessage(TopicPartition tp, UUID messageId) {
    QueuedMap<UUID, Long> offsetMapForPartition = _offsetMap.get(tp);
    if (offsetMapForPartition != null) {
      offsetMapForPartition.remove(messageId);
    } else {
      throw new OffsetNotTrackedException("Could not find message " + messageId + " in partition " + tp
          + ". This should not happen because a completed large message must have been tracked by offset "
          + "tracker!");
    }
  }

  /**
   * If the specified messageId has not been seen before then track the associated offset as its starting offset.
   */
  void trackMessage(TopicPartition tp, UUID messageId, long offset) {
    QueuedMap<UUID, Long> offsetMapForPartition = getAndMaybePutOffsetMapForPartition(tp);
    if (offsetMapForPartition.get(messageId) == null) {
      offsetMapForPartition.put(messageId, offset);
    }
  }

  List<UUID> expireMessageUntilOffset(TopicPartition tp, long offset) {
    List<UUID> expired = new ArrayList<>();
    QueuedMap<UUID, Long> offsetMapForPartition = _offsetMap.get(tp);
    UUID eldest = offsetMapForPartition == null ? null : offsetMapForPartition.getEldestKey();
    while (eldest != null && offsetMapForPartition.get(eldest) < offset) {
      expired.add(eldest);
      offsetMapForPartition.remove(eldest);
      eldest = offsetMapForPartition.getEldestKey();
    }
    return expired;
  }

  void clear() {
    _offsetMap.clear();
  }

  void clear(TopicPartition tp) {
    _offsetMap.remove(tp);
  }

  private QueuedMap<UUID, Long> getAndMaybePutOffsetMapForPartition(TopicPartition tp) {
    QueuedMap<UUID, Long> offsetMapForPartition = _offsetMap.get(tp);
    if (offsetMapForPartition == null) {
      offsetMapForPartition = new QueuedMap<>();
      _offsetMap.put(tp, offsetMapForPartition);
    }
    return offsetMapForPartition;
  }
}
