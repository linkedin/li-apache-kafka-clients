/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.metadataservice;


public class MetadataServiceClientException extends Exception {
  public MetadataServiceClientException(String message) {
    super(message);
  }

  public MetadataServiceClientException(Throwable cause) {
    super(cause);
  }

  public MetadataServiceClientException(String message, Throwable cause) {
    super(message, cause);
  }
}
