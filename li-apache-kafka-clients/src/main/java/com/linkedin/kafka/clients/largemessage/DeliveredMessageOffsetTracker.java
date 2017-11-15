/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.largemessage.errors.OffsetNotTrackedException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is only used along with large message support. It is to support the following use case:
 * 1. User consumed a batch of ConsumerRecords,
 * 2. User processed some consumer records in the batch and wanted to commit offsets.
 * In this case, we need to guarantee that the all the message delivered after the committed offsets will be consumed
 * again if user resume consumer later. In this case, the safe offsets map returned by large message pool is not
 * sufficient, because it is the safe boundary after the entire batch was delivered, not the safe boundary in the
 * middle of the batch.
 * This class will keep the safe boundary of N recently delivered messages for each partition. When user try to commit
 * offset X, it will be able to find the corresponding safe boundary when message with offset X was delivered.
 */
public class DeliveredMessageOffsetTracker {
  private static final Logger LOG = LoggerFactory.getLogger(DeliveredMessageOffsetTracker.class);
  private final int _maxMessagesToTrack;
  private final Map<TopicPartition, PartitionOffsetTracker> _offsetTrackerMap;

  public DeliveredMessageOffsetTracker(int maxMessagesToTrack) {
    _maxMessagesToTrack = maxMessagesToTrack;
    _offsetTrackerMap = new ConcurrentHashMap<>();
  }

  /**
   * Track the safe offset and starting offset of the given message.
   *
   * @param tp                    the partition the message is from
   * @param messageOffset         the offset of the message
   * @param safeOffset            the safe offset when this message is delivered
   * @param messageStartingOffset the starting offset of this message.
   * @param delivered             whether the message was delivered.
   */
  public void track(TopicPartition tp, long messageOffset, long safeOffset, long messageStartingOffset, boolean delivered) {
    LOG.debug("Tracking offset for partition {}: messageOffset = {}, safeOffset = {}, messageStartingOffset = {}",
              tp, messageOffset, safeOffset, messageStartingOffset);
    PartitionOffsetTracker offsetTracker = getAndMaybeCreateOffsetTracker(tp, messageOffset);
    offsetTracker.updateCurrentSafeOffset(safeOffset);
    // Only update the delivered offset if the message was delivered.
    if (delivered) {
      offsetTracker.updateDelivered(messageOffset);
    }

    if (messageOffset != messageStartingOffset || safeOffset != messageOffset + 1) {
      MessageStartingAndSafeOffset messageOffsetInfo = new MessageStartingAndSafeOffset(safeOffset, messageStartingOffset);
      offsetTracker.put(messageOffset, messageOffsetInfo);
      LOG.trace("Tracked message({}): {}. Tracked Offset Range: [{}, {}]", messageOffset, messageOffsetInfo,
                offsetTracker.earliestTrackedDeliveredOffset(), offsetTracker.delivered());
    }
  }

  /**
   * Add a non message offset to track, i.e. a large message segment.
   *
   * @param tp the topic partition of a
   * @param offset the offset of the non large message.
   */
  public void addNonMessageOffset(TopicPartition tp, long offset) {
    PartitionOffsetTracker offsetTracker = getAndMaybeCreateOffsetTracker(tp, offset);
    offsetTracker.trackNonMessageOffsets(Collections.singleton(offset));
  }

  private PartitionOffsetTracker getAndMaybeCreateOffsetTracker(TopicPartition tp, long offset) {
    return _offsetTrackerMap.computeIfAbsent(tp, topicPartition -> new PartitionOffsetTracker(offset));
  }

  public Long safeOffset(TopicPartition tp) {
    PartitionOffsetTracker offsetTracker = _offsetTrackerMap.get(tp);
    return offsetTracker == null ? null : offsetTracker.currentSafeOffset();
  }

  public Long safeOffset(TopicPartition tp, long messageOffset) {
    PartitionOffsetTracker offsetTracker = _offsetTrackerMap.get(tp);
    // If the message offset is negative, we simply return Long.MaxValue.
    if (messageOffset < 0 || offsetTracker == null) {
      // No message has been consumed for this topic partition. Accept whatever offset user provided.
      return null;
    } else if (!offsetTracker.isTrackedMessageOffset(messageOffset)) {
      // Message offset has been evicted.
      throw new OffsetNotTrackedException("Offset " + messageOffset + " for partition " + tp
                                              + " is either invalid or has been evicted. Tracked Offset Range: ["
                                              + offsetTracker.earliestTrackedDeliveredOffset()
                                              + ", " + offsetTracker.delivered() + "], SafeOffset = "
                                              + offsetTracker.currentSafeOffset());
    }
    return offsetTracker.get(messageOffset).safeOffset;
  }

  public Map<TopicPartition, Long> safeOffsets() {
    Map<TopicPartition, Long> safeOffsetMap = new HashMap<>();
    for (Map.Entry<TopicPartition, PartitionOffsetTracker> entry : _offsetTrackerMap.entrySet()) {
      safeOffsetMap.put(entry.getKey(), entry.getValue().currentSafeOffset());
    }
    return safeOffsetMap;
  }

  public long startingOffset(TopicPartition tp, long messageOffset) {
    PartitionOffsetTracker offsetTracker = _offsetTrackerMap.get(tp);
    if (!hasDeliveredMessages(offsetTracker)) {
      // No message delivered for this topic partition. The starting offset is the message offset.
      return messageOffset;
    } else if (!offsetTracker.isTrackedMessageOffset(messageOffset)) {
      // Message offset has been evicted.
      throw new OffsetNotTrackedException("Offset " + messageOffset + " for partition " + tp
                                              + " is either invalid or has been evicted. Tracked Offset Range: ["
                                              + offsetTracker.earliestTrackedDeliveredOffset()
                                              + ", " + offsetTracker.delivered() + "], SafeOffset = "
                                              + offsetTracker.currentSafeOffset());
    }
    return offsetTracker.get(messageOffset).messageStartingOffset;
  }

  public Long closestDeliveredUpTo(TopicPartition tp, long upToMessageOffset) {
    PartitionOffsetTracker offsetTracker = _offsetTrackerMap.get(tp);
    if (!hasDeliveredMessages(offsetTracker)) {
      return null;
    } else if (!offsetTracker.isOffsetInTrackedRange(upToMessageOffset)) {
      // Message offset has been evicted.
      throw new OffsetNotTrackedException("Most recently delivered message for partition " + tp + " before offset "
                                              + upToMessageOffset + " has been evicted. Tracked Offset Range: ["
                                              + offsetTracker.earliestTrackedDeliveredOffset()
                                              + ", " + offsetTracker.delivered() + "]");
    }

    return offsetTracker.closestDeliveredUpTo(upToMessageOffset);
  }

  public Long delivered(TopicPartition tp) {
    PartitionOffsetTracker offsetTracker = _offsetTrackerMap.get(tp);
    if (!hasDeliveredMessages(offsetTracker)) {
      return null;
    } else {
      return offsetTracker.delivered();
    }
  }

  public Map<TopicPartition, Long> delivered() {
    Map<TopicPartition, Long> deliveredMap = new HashMap<>();
    for (Map.Entry<TopicPartition, PartitionOffsetTracker> entry : _offsetTrackerMap.entrySet()) {
      if (hasDeliveredMessages(entry.getValue())) {
        deliveredMap.put(entry.getKey(), entry.getValue().delivered());
      }
    }
    return deliveredMap;
  }

  public Long earliestTrackedOffset(TopicPartition tp) {
    PartitionOffsetTracker offsetTracker = _offsetTrackerMap.get(tp);
    return offsetTracker == null ? null : offsetTracker.earliestTrackedOffset();
  }

  public void clear() {
    _offsetTrackerMap.clear();
  }

  public void clear(TopicPartition tp) {
    _offsetTrackerMap.remove(tp);
  }

  private boolean hasDeliveredMessages(PartitionOffsetTracker offsetTracker) {
    return offsetTracker != null && offsetTracker.delivered() > 0;
  }

  private class PartitionOffsetTracker extends LinkedHashMap<Long, MessageStartingAndSafeOffset> {
    volatile private long _earliestTrackedOffset;
    private long _currentSafeOffset;
    private Long _delivered;
    private TreeSet<Long> _nonMessageOffsets;

    PartitionOffsetTracker(Long earliestTrackedOffset) {
      _currentSafeOffset = earliestTrackedOffset;
      _earliestTrackedOffset = earliestTrackedOffset;
      _nonMessageOffsets = new TreeSet<>();
      _delivered = -1L;
    }

    void updateCurrentSafeOffset(long currentSafeOffset) {
      _currentSafeOffset = currentSafeOffset;
    }

    void updateDelivered(long delivered) {
      _delivered = delivered;
    }

    long currentSafeOffset() {
      return _currentSafeOffset;
    }

    long earliestTrackedOffset() {
      return _earliestTrackedOffset;
    }

    /**
     * This method will return the first delivered message offset by excluding the leading non message offsets.
     * It should only used by logging.
     */
    long earliestTrackedDeliveredOffset() {
      long earliestTrackedOffset = _earliestTrackedOffset;
      while (_nonMessageOffsets.contains(earliestTrackedOffset)) {
        earliestTrackedOffset++;
      }
      return earliestTrackedOffset;
    }

    void trackNonMessageOffsets(Set<Long> offsets) {
      _nonMessageOffsets.addAll(offsets);
    }

    boolean isTrackedMessageOffset(long messageOffset) {
      return messageOffset >= _earliestTrackedOffset
          && messageOffset <= _delivered
          && !_nonMessageOffsets.contains(messageOffset);
    }

    boolean isOffsetInTrackedRange(long messageOffset) {
      return messageOffset >= _earliestTrackedOffset && messageOffset <= _delivered;
    }

    Long delivered() {
      return _delivered;
    }

    Long closestDeliveredUpTo(long upToMessageOffset) {
      long closestSmallerMessageOffset = upToMessageOffset;
      while (_nonMessageOffsets.contains(closestSmallerMessageOffset)) {
        closestSmallerMessageOffset--;
      }
      return closestSmallerMessageOffset < _earliestTrackedOffset ? null : closestSmallerMessageOffset;
    }

    @Override
    public MessageStartingAndSafeOffset get(Object key) {
      Long messageOffset = (Long) key;
      assert (!_nonMessageOffsets.contains(messageOffset));
      MessageStartingAndSafeOffset messageStartingAndSafeOffset = super.get(messageOffset);
      if (messageStartingAndSafeOffset == null) {
        messageStartingAndSafeOffset = new MessageStartingAndSafeOffset(messageOffset + 1, messageOffset);
      }
      return messageStartingAndSafeOffset;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Long, MessageStartingAndSafeOffset> entry) {
      boolean shouldRemove = this.size() > _maxMessagesToTrack;
      if (shouldRemove) {
        _earliestTrackedOffset = entry.getKey() + 1;
        // Remove all the old offsets from the non-message set.
        Iterator<Long> iter = _nonMessageOffsets.iterator();
        while (iter.hasNext()) {
          long offset = iter.next();
          if (offset < entry.getKey()) {
            iter.remove();
          } else {
            break;
          }
        }
        LOG.trace("Removed message({}) from delivered message offset tracker. New earliest tracked offset = {}, " +
                      "total message tracked = {}, total invalid offsets = {}", entry.getKey(), _earliestTrackedOffset,
                  this.size(), _nonMessageOffsets.size());
      }
      return shouldRemove;
    }
  }

  private class MessageStartingAndSafeOffset {
    final long safeOffset;
    final long messageStartingOffset;

    MessageStartingAndSafeOffset(long safeOffset, long messageStartingOffset) {
      this.safeOffset = safeOffset;
      this.messageStartingOffset = messageStartingOffset;
    }

    @Override
    public String toString() {
      return String.format("{safeOffset = %d, startingOffset = %d}", safeOffset, messageStartingOffset);
    }
  }
}
