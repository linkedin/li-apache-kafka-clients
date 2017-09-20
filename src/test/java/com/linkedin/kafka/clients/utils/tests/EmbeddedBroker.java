/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
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
  private final static Pattern LISTENER_PATTERN = Pattern.compile("\\s*(\\w+)://([\\w\\d]+):(\\d+)\\s*");

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
      Method emptyOptionMethod = SCALA_OPTION_CLASS.getMethod("empty");
      EMPTY_OPTION = emptyOptionMethod.invoke(null);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private String id;
  private File logDir;
  private String plaintextHost = null;
  private int plaintextPort = -1;
  private String sslHost = null;
  private int sslPort = -1;
  private Object serverInstance;

  public EmbeddedBroker(Map<Object, Object> config) {
    try {
      Object configInstance = CONFIG_CTR.newInstance(config); //also validates
      parseConfigs(config);
      Object emptyArrayBuffer = ARR_BUF_CTR.newInstance();
      serverInstance = SERVER_CTR.newInstance(configInstance, Time.SYSTEM, EMPTY_OPTION, emptyArrayBuffer);
      STARTUP_METHOD.invoke(serverInstance);
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
      Matcher m = LISTENER_PATTERN.matcher(protocolAddr);
      if (!m.matches()) {
        throw new IllegalStateException();
      }
      SecurityProtocol protocol = SecurityProtocol.forName(m.group(1));
      String host = m.group(2);
      int port = Integer.parseInt(m.group(3));
      switch (protocol) {
        case PLAINTEXT:
          plaintextHost = host;
          plaintextPort = port;
          break;
        case SSL:
          sslHost = host;
          sslPort = port;
          break;
        default:
          throw new IllegalStateException("unhandled: " + protocol + " in " + protocolAddr);
      }
    }
  }

  public String getId() {
    return id;
  }

  public String getAddr(SecurityProtocol protocol) {
    switch (protocol) {
      case PLAINTEXT:
        return getPlaintextAddr();
      case SSL:
        return getSslAddr();
      default:
        throw new IllegalStateException("unhandled: " + protocol);
    }
  }

  public String getPlaintextAddr() {
    if (plaintextHost == null) {
      return null;
    }
    return plaintextHost + ":" + plaintextPort;
  }

  public String getSslAddr() {
    if (sslHost == null) {
      return null;
    }
    return sslHost + ":" + sslPort;
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
