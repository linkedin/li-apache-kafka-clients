/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;


// A generic descriptor for a cluster
public class ClusterDescriptor {
  private final String _name;
  private final String _bootstrapURL;
  private final String _zkConnection;

  public ClusterDescriptor(String name, String bootstrapURL, String zkConnection) {
    _name = name;
    _bootstrapURL = bootstrapURL;
    _zkConnection = zkConnection;
  }

  public String name() {
    return _name;
  }

  public String bootstrapURL() {
    return _bootstrapURL;
  }

  public String zkConnection() {
    return _zkConnection;
  }

  public String toString() {
    return "name: " + _name + ", bootstrapURL: " + _bootstrapURL + ", zkConnection: " + _zkConnection;
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
    return _name == other.name() && _bootstrapURL == other.bootstrapURL() && _zkConnection == other.zkConnection();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}