/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.protocol.SecurityProtocol;


public class EmbeddedBrokerBuilder {
  private final static AtomicInteger BROKER_ID_COUNTER = new AtomicInteger();

  //mandatory fields
  private int nodeId = BROKER_ID_COUNTER.incrementAndGet();
  private String zkConnect;
  //storage config
  private File logDirectory;
  //networking config
  private int plaintextPort = -1;
  private int sslPort = -1;
  private SecurityProtocol interBrokerSecurity;
  private File trustStore;
  private long socketTimeout = 1500;
  //feature control
  private boolean enableControlledShutdown;
  private long controlledShutdownRetryBackoff = 100;
  private boolean enableDeleteTopic;
  private boolean enableLogCleaner;
  //resource management
  private long logCleanerDedupBufferSize = 2097152; //2MB
  private String rack;

  //builder state
  boolean plaintextPortSet = false;
  boolean sslPortSet = false;

  public EmbeddedBrokerBuilder() {
  }

  public EmbeddedBrokerBuilder nodeId(int nodeId) {
    this.nodeId = nodeId;
    return this;
  }

  public EmbeddedBrokerBuilder zkConnect(String zkConnect) {
    this.zkConnect = zkConnect;
    return this;
  }

  public EmbeddedBrokerBuilder zkConnect(EmbeddedZookeeper zk) {
    return zkConnect(zk.getConnectionString());
  }

  public EmbeddedBrokerBuilder logDirectory(File logDirectory) {
    this.logDirectory = logDirectory;
    return this;
  }

  public EmbeddedBrokerBuilder enable(SecurityProtocol protocol) {
    switch (protocol) {
      case PLAINTEXT:
        enablePlaintext();
        break;
      case SSL:
        enableSsl();
        break;
      default:
        throw new IllegalStateException("unhandled: " + protocol);
    }
    return this;
  }

  public EmbeddedBrokerBuilder plaintextPort(int plaintextPort) {
    this.plaintextPort = plaintextPort;
    plaintextPortSet = true;
    return this;
  }

  public EmbeddedBrokerBuilder enablePlaintext() {
    return plaintextPort(0);
  }

  public EmbeddedBrokerBuilder sslPort(int sslPort) {
    this.sslPort = sslPort;
    sslPortSet = true;
    return this;
  }

  public EmbeddedBrokerBuilder enableSsl() {
    return sslPort(0);
  }

  public EmbeddedBrokerBuilder interBrokerSecurity(SecurityProtocol interBrokerSecurity) {
    this.interBrokerSecurity = interBrokerSecurity;
    if (interBrokerSecurity == null) {
      if (!plaintextPortSet) {
        plaintextPort = -1;
      }
      if (!sslPortSet) {
        sslPort = -1;
      }
    } else {
      switch (interBrokerSecurity) {
        case PLAINTEXT:
          if (!plaintextPortSet) {
            plaintextPort = 0;
          }
          break;
        case SSL:
          if (!sslPortSet) {
            sslPort = 0;
          }
          break;
        default:
          throw new UnsupportedOperationException("unhandled: " + interBrokerSecurity);
      }
    }
    return this;
  }

  public EmbeddedBrokerBuilder trustStore(File trustStore) {
    this.trustStore = trustStore;
    return this;
  }

  public EmbeddedBrokerBuilder socketTimeout(long socketTimeout) {
    this.socketTimeout = socketTimeout;
    return this;
  }

  public EmbeddedBrokerBuilder enableControlledShutdown(boolean enableControlledShutdown) {
    this.enableControlledShutdown = enableControlledShutdown;
    return this;
  }

  public EmbeddedBrokerBuilder controlledShutdownRetryBackoff(long controlledShutdownRetryBackoff) {
    this.controlledShutdownRetryBackoff = controlledShutdownRetryBackoff;
    return this;
  }

  public EmbeddedBrokerBuilder enableDeleteTopic(boolean enableDeleteTopic) {
    this.enableDeleteTopic = enableDeleteTopic;
    return this;
  }

  public EmbeddedBrokerBuilder enableLogCleaner(boolean enableLogCleaner) {
    this.enableLogCleaner = enableLogCleaner;
    return this;
  }

  public EmbeddedBrokerBuilder logCleanerDedupBufferSize(long logCleanerDedupBufferSize) {
    this.logCleanerDedupBufferSize = logCleanerDedupBufferSize;
    return this;
  }

  public EmbeddedBrokerBuilder rack(String rack) {
    this.rack = rack;
    return this;
  }

  private void applyDefaults() {
    if (logDirectory == null) {
      logDirectory = KafkaTestUtils.newTempDir();
    }
  }

  private void validate() throws IllegalArgumentException {
    if (plaintextPort < 0 && sslPort < 0) {
      throw new IllegalArgumentException("at least one protocol must be used");
    }
    if (logDirectory == null) {
      throw new IllegalArgumentException("log directory must be specified");
    }
    if (zkConnect == null) {
      throw new IllegalArgumentException("zkConnect must be specified");
    }
  }

  public Map<Object, Object> buildConfig() {
    applyDefaults();
    validate();

    Map<Object, Object> props = new HashMap<>();

    StringJoiner csvJoiner = new StringJoiner(",");
    if (plaintextPort >= 0) {
      csvJoiner.add(SecurityProtocol.PLAINTEXT.name + "://localhost:" + plaintextPort);
    }
    if (sslPort >= 0) {
      csvJoiner.add(SecurityProtocol.SSL.name + "://localhost:" + sslPort);
    }
    props.put("broker.id", Integer.toString(nodeId));
    props.put("listeners", csvJoiner.toString());
    props.put("log.dir", logDirectory.getAbsolutePath());
    props.put("zookeeper.connect", zkConnect);
    props.put("replica.socket.timeout.ms", Long.toString(socketTimeout));
    props.put("controller.socket.timeout.ms", Long.toString(socketTimeout));
    props.put("controlled.shutdown.enable", Boolean.toString(enableControlledShutdown));
    props.put("delete.topic.enable", Boolean.toString(enableDeleteTopic));
    props.put("controlled.shutdown.retry.backoff.ms", Long.toString(controlledShutdownRetryBackoff));
    props.put("log.cleaner.dedupe.buffer.size", Long.toString(logCleanerDedupBufferSize));
    props.put("log.cleaner.enable", Boolean.toString(enableLogCleaner));
    props.put("offsets.topic.replication.factor", "1");
    if (rack != null) {
      props.put("broker.rack", rack);
    }
    if (trustStore != null || sslPort > 0) {
      try {
        props.putAll(TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStore, "server" + nodeId));
        //switch interbroker to ssl
        props.put("security.inter.broker.protocol", "SSL");
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    return props;
  }

  public EmbeddedBroker build() {
    return new EmbeddedBroker(buildConfig());
  }
}
