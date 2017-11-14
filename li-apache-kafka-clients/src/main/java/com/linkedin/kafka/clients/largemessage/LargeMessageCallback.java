/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.largemessage.errors.LargeMessageSendException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * This is the callback class for large message. It works in the following way:
 * 1. It holds the original callback provided by user.
 * 2. The user callback will only fire once When all the segments of the large messages are acked. If all the segments
 * are sent successfully, the user callback will receive no exception. If exceptions are received by several
 * segments, the user callback will receive the first exception.
 * Because the callback will only be called by one single thread, no synchronization is needed.
 */
public class LargeMessageCallback implements Callback {
  private final int _numSegments;
  private final Callback _userCallback;
  private int _acksReceived;
  private int _segmentsSent;
  private Exception _exception;

  public LargeMessageCallback(int numSegments, Callback userCallback) {
    _numSegments = numSegments;
    _acksReceived = 0;
    _segmentsSent = 0;
    _userCallback = userCallback;
    _exception = null;
  }

  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
    // The callback will only be fired once.
    _acksReceived++;

    // Set exception to be the first exception
    if (e != null && _exception == null) {
      _exception = e;
    }
    if (e == null) {
      _segmentsSent++;
    }
    // Invoke user callback when receive the last callback of the large message.
    if (_acksReceived == _numSegments) {
      if (_exception == null) {
        _userCallback.onCompletion(recordMetadata, null);
      } else {
        _userCallback.onCompletion(
            null,
            new LargeMessageSendException(String.format("Error when sending large message. Sent %d of %d segments.",
                _segmentsSent, _numSegments), _exception)
        );
      }
    }
  }
}
