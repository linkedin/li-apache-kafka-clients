/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Time;


public class EmbeddedBroker implements AutoCloseable {
  private final static Class<?> KAFKA_CONFIG_CLASS;
  private final static Class<?> KAFKA_SERVER_CLASS;
  private final static Class<?> SCALA_OPTION_CLASS;
  private final static Class<?> SCALA_SEQ_CLASS;
  private final static Class<?> SCALA_ARRAY_BUFFER_CLASS;
  private final static Constructor<?> CONFIG_CTR;
  private final static Constructor<?> SERVER_CTR;
  private final static Constructor<?> ARR_BUF_CTR;
  private final static Method STARTUP_METHOD;
  private final static Method SHUTDOWN_METHOD;
  private final static Method AWAIT_SHUTDOWN_METHOD;
  private final static Object EMPTY_OPTION;
  private final static Method BOUND_PORT_METHOD;

  static {
    try {
      KAFKA_CONFIG_CLASS = Class.forName("kafka.server.KafkaConfig");
      KAFKA_SERVER_CLASS = Class.forName("kafka.server.KafkaServer");
      SCALA_OPTION_CLASS = Class.forName("scala.Option");
      SCALA_SEQ_CLASS = Class.forName("scala.collection.Seq");
      SCALA_ARRAY_BUFFER_CLASS = Class.forName("scala.collection.mutable.ArrayBuffer");
      CONFIG_CTR = KAFKA_CONFIG_CLASS.getConstructor(Map.class);
      SERVER_CTR = KAFKA_SERVER_CLASS.getConstructor(KAFKA_CONFIG_CLASS, Time.class, SCALA_OPTION_CLASS, SCALA_SEQ_CLASS);
      ARR_BUF_CTR = SCALA_ARRAY_BUFFER_CLASS.getConstructor();
      STARTUP_METHOD = KAFKA_SERVER_CLASS.getMethod("startup");
      SHUTDOWN_METHOD = KAFKA_SERVER_CLASS.getMethod("shutdown");
      AWAIT_SHUTDOWN_METHOD = KAFKA_SERVER_CLASS.getMethod("awaitShutdown");
      BOUND_PORT_METHOD = KAFKA_SERVER_CLASS.getMethod("boundPort", ListenerName.class);
      Method emptyOptionMethod = SCALA_OPTION_CLASS.getMethod("empty");
      EMPTY_OPTION = emptyOptionMethod.invoke(null);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private String id;
  private File logDir;
  private Map<SecurityProtocol, Integer> ports = new HashMap<>();
  private Map<SecurityProtocol, String> hosts = new HashMap<>();
  private Object serverInstance;

  public EmbeddedBroker(Map<Object, Object> config) {
    try {
      Object configInstance = CONFIG_CTR.newInstance(config); //also validates
      parseConfigs(config);
      Object emptyArrayBuffer = ARR_BUF_CTR.newInstance();
      serverInstance = SERVER_CTR.newInstance(configInstance, Time.SYSTEM, EMPTY_OPTION, emptyArrayBuffer);
      STARTUP_METHOD.invoke(serverInstance);
      ports.replaceAll((securityProtocol, port) -> {
        try {
          return (Integer) BOUND_PORT_METHOD.invoke(serverInstance, ListenerName.forSecurityProtocol(securityProtocol));
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      });
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void parseConfigs(Map<Object, Object> config) {
    id = (String) config.get("broker.id");
    logDir = new File((String) config.get("log.dir"));

    //bind addresses
    String listenersString = (String) config.get("listeners");
    for (String protocolAddr : listenersString.split("\\s*,\\s*")) {
      try {
        URI uri = new URI(protocolAddr.trim());
        SecurityProtocol protocol = SecurityProtocol.forName(uri.getScheme());
        hosts.put(protocol, uri.getHost());
        ports.put(protocol, null); //we get the value after boot
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  public String getId() {
    return id;
  }

  public String getAddr(SecurityProtocol protocol) {
    if (!hosts.containsKey(protocol)) {
      return null;
    }
    return hosts.get(protocol) + ":" + ports.get(protocol);
  }

  public String getPlaintextAddr() {
    return getAddr(SecurityProtocol.PLAINTEXT);
  }

  public String getSslAddr() {
    return getAddr(SecurityProtocol.SSL);
  }

  public void shutdown() throws Exception {
    SHUTDOWN_METHOD.invoke(serverInstance);
  }

  @Override
  public void close() throws Exception {
    KafkaTestUtils.quietly(() -> SHUTDOWN_METHOD.invoke(serverInstance));
    KafkaTestUtils.quietly(() -> AWAIT_SHUTDOWN_METHOD.invoke(serverInstance));
    KafkaTestUtils.quietly(() -> FileUtils.forceDelete(logDir));
  }

  public static EmbeddedBrokerBuilder newServer() {
    return new EmbeddedBrokerBuilder();
  }
}
