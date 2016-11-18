package com.linkedin.kafka.clients.utils;

/**
 * This is used by large message support in the case where we key and partition are null.
 * The SimplePartitioner just looks at the topic and does not need the complete Cluster state which is private.
 */
public interface SimplePartitioner {
  int partition(String topic);
}
