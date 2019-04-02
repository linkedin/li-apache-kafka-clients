/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import java.util.Objects;


// A generic descriptor for a cluster group
public class ClusterGroupDescriptor {
  private final String _name;
  private final String _environment;

  public ClusterGroupDescriptor(String name, String environment) {
    _name = name;
    _environment = environment;
  }

  public String getName() {
    return _name;
  }

  public String getEnvironment() {
    return _environment;
  }

  public String toString() {
    return _name + "@" + _environment;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusterGroupDescriptor other = (ClusterGroupDescriptor) o;
    return _name == other.getName() && _environment == other.getEnvironment();
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _environment);
  }
}
