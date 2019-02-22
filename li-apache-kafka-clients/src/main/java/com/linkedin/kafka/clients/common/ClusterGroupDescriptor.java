/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;


// A generic descriptor for a cluster group
public class ClusterGroupDescriptor {
  private final String _environment;
  private final String _name;

  public ClusterGroupDescriptor(String environment, String name) {
    _environment = environment;
    _name = name;
  }

  public String environment() {
    return _environment;
  }

  public String name() {
    return _name;
  }

  public String toString() {
    return _name + "@" + _environment;
  }
}
