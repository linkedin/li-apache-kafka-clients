/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
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
public class LargeMessageOffsetTracker {

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

  void maybeTrackMessage(TopicPartition tp, UUID messageId, long offset) {
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
