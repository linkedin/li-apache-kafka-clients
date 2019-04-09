/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import java.util.Objects;


// A generic descriptor for a cluster
public class ClusterDescriptor {
  private final String _name;
  private final String _bootstrapUrl;
  private final String _zkConnection;

  public ClusterDescriptor(String name, String bootstrapUrl, String zkConnection) {
    _name = name;
    _bootstrapUrl = bootstrapUrl;
    _zkConnection = zkConnection;
  }

  public String getName() {
    return _name;
  }

  public String getBootstrapUrl() {
    return _bootstrapUrl;
  }

  public String getZkConnection() {
    return _zkConnection;
  }

  public String toString() {
    return _name + " (bootstrap: " + _bootstrapUrl + ", zk: " + _zkConnection + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusterDescriptor other = (ClusterDescriptor) o;
    return _name.equals(other.getName()) && _bootstrapUrl.equals(other.getBootstrapUrl()) &&
        _zkConnection.equals(other.getZkConnection());
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _bootstrapUrl, _zkConnection);
  }
}