/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.mario.client.LoggingHandler;
import org.slf4j.Logger;


/**
 * logging handler for instrumented clients to log connection issues
 * to conductor.
 * since conductor is an optional dependency for these clients the
 * logging performed is less verbose and emits warnings instead
 * of errors (which is what the default conductor client would do)
 */
public class InstrumentedClientLoggingHandler implements LoggingHandler {
  private final Logger logger;
  private final String clientId;
  private final String prefix;

  public InstrumentedClientLoggingHandler(Logger logger, String clientId) {
    if (logger == null) {
      throw new IllegalArgumentException("must provide logger");
    }
    this.logger = logger;
    this.clientId = clientId;
    this.prefix = this.clientId == null ? "" : ("client " + clientId + " ");
  }

  @Override
  public void logConnectionFailure(
      String lastAttemptedWebsocketUrl,
      Throwable issue,
      long failureTime,
      int numConsecutiveFailures,
      long delayToNextAttempt) {
    if (numConsecutiveFailures == 1) {
      Throwable root = LiKafkaClientsUtils.getRootCause(issue); //could be null
      String rootDesc = root == null ? "" : (" (" + root.getMessage() + ")");
      //only log on 1st failure, log a warning, and dont display full stack trace
      if (lastAttemptedWebsocketUrl == null || lastAttemptedWebsocketUrl.isEmpty()) {
        logger.warn("{}unable to locate conductor{}. will keep retrying in the background", prefix, rootDesc);
      } else {
        logger.warn("{}unable to open websocket to conductor at {}{}. will keep retrying in the background",
            prefix, lastAttemptedWebsocketUrl, rootDesc);
      }
    }
  }
}
