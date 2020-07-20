/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import com.linkedin.kafka.clients.common.LargeMessageHeaderValue;
import com.linkedin.mario.common.versioning.VersioningUtils;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Util class for likafka-clients.
 */
public class LiKafkaClientsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaClientsUtils.class);
  private static final List<String> KNOWN_PROJECT_GROUPS = Arrays.asList(
      "com.linkedin.kafka",
      "com.linkedin.mario",
      "com.linkedin.kafka.clients"
  );

  private static SecureRandom _secureRandom;

  static {
    try {
      _secureRandom = SecureRandom.getInstance("SHA1PRNG");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Cannot get random UUID due to", e);
    }
  }

  private LiKafkaClientsUtils() {
  }

  public static UUID randomUUID() {
    byte[] bytes = new byte[16];
    _secureRandom.nextBytes(bytes);
    // Set UUID version number 4
    bytes[6] &= 0x0f;
    bytes[6] |= 0x40;
    // Set IETF variant
    bytes[8] &= 0x3f;
    bytes[8] |= 0x80;
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
  }

  public static byte[] uuidToBytes(UUID uuid) {
    byte[] bytes = new byte[16];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putLong(uuid.getMostSignificantBits());
    byteBuffer.putLong(uuid.getLeastSignificantBits());
    return bytes;
  }

  /**
   * Get the user offset from the metadata of the committed offsets.
   * @param metadata the associated metadata.
   * @return the committed user offset, null if there is no such offset. (i.e. the metadata is not committed by
   *         a LiKafkaConsumer.
   */
  public static Long offsetFromWrappedMetadata(String metadata) {
    // handle the offset committed by raw KafkaConsumers
    if (metadata == null) {
      return null;
    } else {
      int separatorIndex = metadata.indexOf(',');
      if (separatorIndex < 0) {
        return null;
      } else {
        try {
          return Long.parseLong(metadata.substring(0, separatorIndex));
        } catch (NumberFormatException nfe) {
          return null;
        }
      }
    }
  }

  public static String metadataFromWrappedMetadata(String metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return metadata;
    } else {
      return metadata.substring(metadata.indexOf(',') + 1);
    }
  }

  public static String wrapMetadataWithOffset(String metadata, long offset) {
    return Long.toString(offset) + "," + metadata;
  }

  // Dump stack traces of all live threads among the given set of threads.
  public static void dumpStacksForAllLiveThreads(Set<Thread> threads) {
    Set<Thread> allLiveThreads = Thread.getAllStackTraces().keySet();
    LOG.error("currently live threads:");
    for (Thread t : threads) {
      if (allLiveThreads.contains(t)) {
        LOG.error("Thread {} (state: {}):", t, t.getState());
        t.dumpStack();
      }
    }
  }

  public static Map<String, String> propertiesToStringMap(Properties props, List<String> errors) {
    if (props == null) {
      return null;
    }
    if (props.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> translated = new HashMap<>(props.size());
    props.forEach((k, v) -> {
      //Properties does not allow for null keys or values
      String sk = k.toString();
      String sv = v.toString();
      String other = translated.put(sk, sv);
      if (other != null && errors != null) {
        errors.add("value " + sk + "=" + sv + " clobbers over value " + other + " after string conversion");
      }
    });
    return translated;
  }

  public static Properties getConsolidatedProperties(Properties props1, Properties props2) {
    Properties consolidated = new Properties();
    if (props1 != null) {
      consolidated.putAll(props1);
    }
    if (props2 != null) {
      consolidated.putAll(props2);
    }
    return consolidated;
  }

  public static Properties convertConfigMapToProperties(Map<String, String> configMap) {
    Properties props = new Properties();
    if (configMap != null) {
      props.putAll(configMap);
    }
    return props;
  }

  public static Map<String, String> getKnownLibraryVersions() {
    Map<String, String> results = new HashMap<>();
    for (String knownProjectGroup : KNOWN_PROJECT_GROUPS) {
      String versions = VersioningUtils.summarize(VersioningUtils.getProjectHistogram(
          project -> project.getCoordinates().getGroup().equals(knownProjectGroup),
          artifact -> artifact.getCoordinates().getVersion()
      ));
      results.put(knownProjectGroup, versions);
    }
    return results;
  }

  public static String getClientId(Consumer<?, ?> consumer) {
    return fishForClientId(consumer.metrics());
  }

  public static String getClientId(Producer<?, ?> producer) {
    return fishForClientId(producer.metrics());
  }

  /**
   * kafka doesnt have an API for getting the client id from a client (WTH?!)
   * relying on reflection is tricky because we may be dealing with various
   * wrappers/decorators, but it does leak through kafka's metrics tags ...
   * @param metrics kafka client metrics
   * @return best guess for the client id
   */
  private static String fishForClientId(Map<MetricName, ? extends Metric> metrics) {
    Set<String> candidates = new HashSet<>();
    metrics.forEach((metricName, metric) -> {
      Map<String, String> tags = metricName.tags();
      if (tags == null) {
        return;
      }
      String clientId = tags.get("client-id");
      if (clientId != null) {
        candidates.add(clientId);
      }
    });
    if (candidates.isEmpty()) {
      return null;
    }
    if (candidates.size() > 1) {
      throw new IllegalArgumentException("ambiguous client id from client: " + candidates);
    }
    return candidates.iterator().next();
  }

  /**
   * Special header keys have a "_" prefix and are managed internally by the clients.
   * @param headers kafka headers object
   * @return any "special" headers container in the argument map
   */
  public static Map<String, byte[]> fetchSpecialHeaders(Headers headers) {
    Map<String, byte[]> map = new HashMap<>();
    for (Header header : headers) {

      if (!header.key().startsWith("_")) {
        // skip any non special header
        continue;
      }

      if (map.containsKey(header.key())) {
        throw new IllegalStateException("Duplicate special header found " + header.key());
      }
      map.put(header.key(), header.value());
    }
    return map;
  }

  /**
   * Fetch value of special timestamp header (_t)
   * @param headers ConsumerRecord headers
   * @return Returns null if _t does not exist otherwise returns the long value
   */
  public static Long fetchTimestampHeader(Headers headers) {
    Map<String, byte[]> specialHeaders = fetchSpecialHeaders(headers);
    return specialHeaders.containsKey(Constants.TIMESTAMP_HEADER)
        ? PrimitiveEncoderDecoder.decodeLong(specialHeaders.get(Constants.TIMESTAMP_HEADER), 0)
        : null;
  }

  /**
   * Fetch value of special large message header (_lm)
   * @param headers ConsumerRecord headers
   * @return Returns null if _lm does not exist otherwise returns the long value
   */
  public static LargeMessageHeaderValue fetchLargeMessageHeader(Headers headers) {
    Map<String, byte[]> specialHeaders = fetchSpecialHeaders(headers);
    return specialHeaders.containsKey(Constants.LARGE_MESSAGE_HEADER)
        ? LargeMessageHeaderValue.fromBytes(specialHeaders.get(Constants.LARGE_MESSAGE_HEADER))
        : null;
  }

  /**
   * Fetch value of special encryption message header (_encrypt)
   * @param headers ConsumerRecord headers
   * @return Returns null if _encrypt does not exist otherwise returns the boolean value
   */
  public static Boolean fetchEncryptionHeader(Headers headers) {
    Map<String, byte[]> specialHeaders = fetchSpecialHeaders(headers);
    return specialHeaders.containsKey(Constants.ENCRYPTION_HEADER)
        ? PrimitiveEncoderDecoder.decodeBoolean(specialHeaders.get(Constants.ENCRYPTION_HEADER), 0)
        : null;
  }
}
