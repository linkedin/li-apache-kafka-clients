/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import java.util.Map;
import java.util.UUID;


// A callback interface for executing commands requested by the metadata service.
//
// A command-specific callback will implement this interface.
public interface FederatedClientCommandCallback {
  public FederatedClientCommandType getCommandType();

  void onReceivingCommand(UUID commandRequestId, Map<String, String> args);
}