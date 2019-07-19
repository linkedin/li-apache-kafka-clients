/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.metadataservice;

import com.linkedin.kafka.clients.common.LiKafkaFederatedClient;
import com.linkedin.kafka.clients.common.LiKafkaFederatedClientType;
import com.linkedin.kafka.clients.consumer.LiKafkaFederatedConsumerImpl;
import com.linkedin.kafka.clients.producer.LiKafkaFederatedProducerImpl;
import com.linkedin.mario.common.websockets.MarioCommandCallback;
import com.linkedin.mario.common.websockets.Messages;

import com.linkedin.mario.common.websockets.RegisterResponseMessages;
import com.linkedin.mario.common.websockets.ReloadConfigRequestMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// This is a callback implementation that will be called from MarioClient side when it receives a command execution
// request from Mario server. It will identify the matching federated client callback for the request and invoke it
// with command-specific arguments.
class MarioCommandCallbackImpl implements MarioCommandCallback {
  private static final Logger LOG = LoggerFactory.getLogger(MarioCommandCallbackImpl.class);

  private LiKafkaFederatedClient _federatedClient;

  MarioCommandCallbackImpl(LiKafkaFederatedClient federatedClient) {
    _federatedClient = federatedClient;
  }

  public void onReceivingCommand(Messages marioCommandMessage) {
    // Find a federate client callback that matches the given Mario command message type and execute it with arguments
    // included in the message.
    LiKafkaFederatedClientType clientType = _federatedClient.getClientType();

    switch (marioCommandMessage.getMsgType()) {
      case RELOAD_CONFIG_REQUEST:
        ReloadConfigRequestMessages reloadConfigMsg = (ReloadConfigRequestMessages) marioCommandMessage;

        if (clientType == LiKafkaFederatedClientType.FEDERATED_PRODUCER) {
          // Call producer reload config method
          ((LiKafkaFederatedProducerImpl) _federatedClient).reloadConfig(reloadConfigMsg.getConfigs(), reloadConfigMsg.getCommandId());
        } else {
          // call consumer reload config method
          ((LiKafkaFederatedConsumerImpl) _federatedClient).reloadConfig(reloadConfigMsg.getConfigs(), reloadConfigMsg.getCommandId());
        }
        break;
      case REGISTER_RESPONSE:
        // Upon receiving register response from conductor, federated client would save the configs from the message and apply the
        // configs when actually creating the per-cluster clients
        RegisterResponseMessages registerResponseMessage = (RegisterResponseMessages) marioCommandMessage;
        if (clientType == LiKafkaFederatedClientType.FEDERATED_PRODUCER) {
          ((LiKafkaFederatedProducerImpl) _federatedClient).applyBootupConfigFromConductor(registerResponseMessage.getConfigs());
        } else {
          ((LiKafkaFederatedConsumerImpl) _federatedClient).applyBootupConfigFromConductor(registerResponseMessage.getConfigs());
        }
        break;
      default:
        // No current support at the moment
        throw new UnsupportedOperationException("command " + marioCommandMessage.getMsgType() + " is not supported");
    }
  }
}
