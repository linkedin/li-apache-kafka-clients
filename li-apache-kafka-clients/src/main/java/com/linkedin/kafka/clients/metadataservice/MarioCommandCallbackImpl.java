/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.metadataservice;

import com.linkedin.kafka.clients.common.FederatedClientCommandCallback;
import com.linkedin.kafka.clients.common.FederatedClientCommandType;
import com.linkedin.mario.common.websockets.MarioCommandCallback;
import com.linkedin.mario.common.websockets.Messages;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// This is a callback implementation that will be called from MarioClient side when it receives a command execution
// request from Mario server. It will identify the matching federated client callback for the request and invoke it
// with command-specific arguments.
class MarioCommandCallbackImpl implements MarioCommandCallback {
  private static final Logger LOG = LoggerFactory.getLogger(MarioCommandCallbackImpl.class);

  private Map<FederatedClientCommandType, FederatedClientCommandCallback> _federatedClientCommandCallbacks;

  MarioCommandCallbackImpl(Collection<FederatedClientCommandCallback> callbacks) {
    for (FederatedClientCommandCallback callback : callbacks) {
      _federatedClientCommandCallbacks.put(callback.getCommandType(), callback);
    }
  }

  public void onReceivingCommand(Messages marioCommandMessage) {
    // Find a federate client callback that matches the given Mario command message type and execute it with arguments
    // included in the message.
    switch (marioCommandMessage.getMsgType()) {
      default:
        // No current support at the moment
        LOG.warn("command {} is unsupported", marioCommandMessage.getMsgType());
    }
  }
}