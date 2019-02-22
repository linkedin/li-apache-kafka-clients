/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;


/**
 * The common configurations for both LiKafkaFederatedProducer and LiKafkaFederatedConsumer
 */
public final class LiKafkaCommonClientConfigs {
  public static final String METADATA_SERVICE_CLIENT_CLASS_CONFIG = "li.metadata.service.client";
  public static final String METADATA_SERVICE_CLIENT_CLASS_DOC = "The metadata service client class";

  public static final String METADATA_SERVICE_REQUEST_TIMEOUT_MS_CONFIG = "li.metadata.service.request.timeout.ms";
  public static final String METADATA_SERVICE_REQUEST_TIMEOUT_MS_DOC =
      "Timeout in milliseconds for requests to the metadata service";

  public static final String CLUSTER_GROUP_CONFIG = "li.cluster.group";
  public static final String CLUSTER_GROUP_DOC = "The name of the cluster group";

  public static final String CLUSTER_ENVIRONMENT_CONFIG = "li.cluster.environment";
  public static final String CLUSTER_ENVIRONMENT_DOC = "The location of the cluster group";

  private LiKafkaCommonClientConfigs() {
    // Not called. Just to avoid style check error.
  }
}
