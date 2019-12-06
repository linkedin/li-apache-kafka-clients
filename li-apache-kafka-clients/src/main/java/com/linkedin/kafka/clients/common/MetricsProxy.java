/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.common;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

/**
 * this class allows delegating kafka metrics to an underlying delegate
 * kafka client, allowing the delegate to be replaced/recreated without
 * "invalidating" metrics maps user code may hold on to. <br >
 * it is meant to allow user code like the following to continue to "just work": <br >
 *
 *    KafkaClient client = ... <br >
 *    Map&lt;MetricName, ? extends Metric&gt; metrics = client.getMetrics(); <br >
 *    // ... long time later ... <br >
 *    //do something with metrics map <br >
 *
 * while still allowing instrumented clients to replace the underlying kafka client
 */
public abstract class MetricsProxy implements Map {

  /**
   * @return the metrics map for the current delegate client, or an empty map if none.
   */
  protected abstract Map<MetricName, ? extends Metric> getMetrics();

  @Override
  public int size() {
    return getMetrics().size();
  }

  @Override
  public boolean isEmpty() {
    return getMetrics().isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return getMetrics().containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return getMetrics().containsValue(value);
  }

  @Override
  public Object get(Object key) {
    return getMetrics().get(key);
  }

  @Override
  public Metric put(Object key, Object value) {
    throw new UnsupportedOperationException(); //this collection is immutable in vanilla kafka anyway
  }

  @Override
  public Object remove(Object key) {
    throw new UnsupportedOperationException(); //this collection is immutable in vanilla kafka anyway
  }

  @Override
  public void putAll(Map m) {
    throw new UnsupportedOperationException(); //this collection is immutable in vanilla kafka anyway
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException(); //this collection is immutable in vanilla kafka anyway
  }

  @Override
  public Set keySet() {
    return getMetrics().keySet();
  }

  @Override
  public Collection values() {
    return getMetrics().values();
  }

  @Override
  public Set entrySet() {
    return getMetrics().entrySet();
  }
}