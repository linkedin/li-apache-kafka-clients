/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.largemessage.LargeMessageCallback;
import com.linkedin.kafka.clients.largemessage.MessageSplitter;
import com.linkedin.kafka.clients.largemessage.MessageSplitterImpl;
import com.linkedin.kafka.clients.utils.HeaderSerializer;
import com.linkedin.kafka.clients.utils.UUIDFactory;
import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.ByteBuffer;

/**
 * This producer is the implementation of {@link LiKafkaProducer}.
 * <p>
 * LiKafkaProducerImpl wraps the vanilla Kafka {@link KafkaProducer Java producer} and provides the following
 * additional functions:
 * <ul>
 * <li>Large message support</li>
 * <li>Auditing</li>
 * </ul>
 * In LiKafkaProducerImpl, a large message (a message which is larger than segment size) will be split into
 * multiple {@link com.linkedin.kafka.clients.largemessage.LargeMessageSegment} and sent to Kafka brokers as individual messages. On the consumer side,
 * LiKafkaConsumer will collect all the segments of the same original large message, reassemble the large message and
 * deliver it to the users.
 * (@see <a href=http://www.slideshare.net/JiangjieQin/handle-large-messages-in-apache-kafka-58692297>design details</a>)
 * <p>
 * Creating a LiKafkaProducerImpl is very similar to creating a {@link KafkaProducer}. User can pass in all
 * the configurations in either a single {@link Properties} or a single {@link Map}. A few additional configurations
 * are required for large message support and auditing. The following example is an extension of the example given in
 * {@link KafkaProducer}.
 * <pre><code>
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("retries", 0);
 * props.put("batch.size", 16384);
 * props.put("linger.ms", 1);
 * props.put("buffer.memory", 33554432);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * // The following properties are used by LiKafkaProducerImpl
 * props.put("large.message.enabled", "true");
 * props.put("max.message.segment.bytes", 1000 * 1024);
 * props.put("auditor.class", LoggingAuditor.class.getName());
 * props.put("uuid.factory.class", UUIDFactoryImpl.class.getName());
 *
 * LiKafkaProducer&lt;String, String&gt; liKafkaProducer = new LiKafkaProducerImpl&lt;&gt;(props);
 * for(int i = 0; i &lt; 100; i++)
 *     liKafkaProducer.send(new ProducerRecord&lt;String, String$gt;("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * </code></pre>
 * <p>
 * User can specify an {@link Auditor} implementation class for LiKafkaProducerImpl. For each ProducerRecord
 * LiKafkaProducer sends, it will audit the ProducerRecord using three predefined {@link AuditType}
 * (ATTEMPT, SUCCESS, FAILURE). User may define more audit types for the auditor if needed.
 * <p>
 * In many cases, after the auditor collected the auditing information, it will send the auditing information out.
 * One option is to send the information to a Kafka topic, which is what we do at LinkedIn. To avoid creating another
 * producer to send the auditing information. LiKafkaClients will pass its underlying vanilla {@link KafkaProducer}
 * to the auditor when invoking {@link Auditor#configure(java.util.Map)}. The auditor implementation can get that
 * producer from the passed in configuration map. For example:
 * <pre><code>
 * {@literal @}Override
 * {@literal @}SuppressWarnings("unchecked")
 * public void configure(Map&lt;String, ?&gt; configs) {
 *    ...
 *    Producer&lt;byte[], byte[]&gt; producer = (Producer&lt;byte[], byte[]&gt;) configs.get(LiKafkaProducerConfig.CURRENT_PRODUCER);
 *    ...
 * }
 * </code></pre>
 * If the underlying KafkaProducer is shared by the auditor implementation. The auditor should not close the shared
 * vanilla producer when {@link Auditor#close()} is invoked.
 */
public class LiKafkaProducerImpl<K, V> implements LiKafkaProducer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaProducerImpl.class);

  // Large message settings
  private final boolean _largeMessageEnabled;
  private final int _maxMessageSegmentSize;
  private final MessageSplitter _messageSplitter;

  // serializers
  private Serializer<K> _keySerializer;
  private Serializer<V> _valueSerializer;

  // raw byte producer
  private final Producer<byte[], byte[]> _producer;
  /*package private for testing*/ Auditor<K, V> _auditor;

  // A counter of the threads in the middle of sending messages. This is needed to ensure when we close the producer
  // everything is audited.
  private final AtomicInteger _numThreadsInSend;
  private final UUIDFactory _uuidFactory;
  private volatile boolean _closed;
  private final HeaderSerializer _headerSerializer;

  public LiKafkaProducerImpl(Properties props) {
    this(new LiKafkaProducerConfig(props), null, null, null);
  }

  public LiKafkaProducerImpl(Properties props,
                             Serializer<K> keySerializer,
                             Serializer<V> valueSerializer,
                             Auditor<K, V> auditor) {
    this(new LiKafkaProducerConfig(props), keySerializer, valueSerializer, auditor);
  }

  public LiKafkaProducerImpl(Map<String, ?> configs) {
    this(new LiKafkaProducerConfig(configs), null,  null, null);
  }

  public LiKafkaProducerImpl(Map<String, ?> configs,
                             Serializer<K> keySerializer,
                             Serializer<V> valueSerializer,
                             Auditor<K, V> auditor) {
    this(new LiKafkaProducerConfig(configs), keySerializer, valueSerializer,  auditor);
  }

  @SuppressWarnings("unchecked")
  private LiKafkaProducerImpl(LiKafkaProducerConfig configs,
                             Serializer<K> keySerializer,
                             Serializer<V> valueSerializer,
                             Auditor<K, V> auditor) {
    // Instantiate the open source producer, which always sends raw bytes.
    _producer = new KafkaProducer<>(configs.originals(), new ByteArraySerializer(), new ByteArraySerializer());

    // Instantiate the key serializer if necessary.
    _keySerializer = keySerializer != null ? keySerializer :
        configs.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
    _keySerializer.configure(configs.originals(), true);
    // Instantiate the key serializer if necessary.
    _valueSerializer = valueSerializer != null ? valueSerializer :
        configs.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
    _valueSerializer.configure(configs.originals(), false);

    // prepare to handle large messages.
    _largeMessageEnabled = configs.getBoolean(LiKafkaProducerConfig.LARGE_MESSAGE_ENABLED_CONFIG);
    _maxMessageSegmentSize = configs.getInt(LiKafkaProducerConfig.MAX_MESSAGE_SEGMENT_BYTES_CONFIG);
    _uuidFactory = configs.getConfiguredInstance(LiKafkaProducerConfig.UUID_FACTORY_CLASS_CONFIG, UUIDFactory.class);

    _messageSplitter = new MessageSplitterImpl(_maxMessageSegmentSize, _uuidFactory);

      // Instantiate auditor if necessary
      _auditor = auditor != null ? auditor
        : configs.getConfiguredInstance(LiKafkaProducerConfig.AUDITOR_CLASS_CONFIG, Auditor.class);
    _auditor.configure(configs.configsWithCurrentProducer(_producer));
    _auditor.start();
    _numThreadsInSend = new AtomicInteger(0);
    _closed = false;

    _headerSerializer =
      configs.getConfiguredInstance(LiKafkaProducerConfig.HEADER_SERIALIZER_CONFIG, HeaderSerializer.class);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
    return send(producerRecord, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
    _numThreadsInSend.incrementAndGet();
    try {
      if (_closed) {
        throw new IllegalStateException("LiKafkaProducer has been closed.");
      }
      String topic = producerRecord.topic();
      K key = producerRecord.key();
      V value = producerRecord.value();
      Object auditToken = _auditor.auditToken(key, value);
      Long timestamp = producerRecord.timestamp() == null ? System.currentTimeMillis() : producerRecord.timestamp();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Sending record with [key={}, value={}] to kafka topic {}",
            key == null ? "[none]" : key,
            value == null ? "[none]" : value,
            topic);
      }
      byte[] serializedValue;
      byte[] serializedKey;
      try {
        serializedValue = _valueSerializer.serialize(topic, value);
        serializedKey = _keySerializer.serialize(topic, key);
      } catch (Exception t) {
        // Audit the attempt and the failure.
        _auditor.record(auditToken, topic, timestamp, 1L, 0L, AuditType.ATTEMPT);
        _auditor.record(auditToken, topic, timestamp, 1L, 0L, AuditType.FAILURE);
        throw new KafkaException(t);
      }

      int sizeInBytes = (serializedKey == null ? 0 : serializedKey.length) + (serializedValue == null ? 0 : serializedValue.length);
      boolean useLargeMessageProcessing = _largeMessageEnabled && serializedValue != null && serializedValue.length > _maxMessageSegmentSize;
      if (!(producerRecord instanceof ExtensibleProducerRecord) && serializedValue == null) {
        return sendNullValue(producerRecord, callback, topic, key, value, timestamp, serializedValue, serializedKey,
          sizeInBytes, auditToken);
      }

      if (producerRecord instanceof ExtensibleProducerRecord && !((ExtensibleProducerRecord) producerRecord).hasHeaders() &&
        serializedValue == null) {
        return sendNullValue(producerRecord, callback, topic, key, value, timestamp, serializedValue, serializedKey,
          sizeInBytes, auditToken);
      }

      ExtensibleProducerRecord<byte[], byte[]> xRecord =
        new ExtensibleProducerRecord<>(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp(),
          serializedKey, serializedValue);
      if (producerRecord instanceof ExtensibleProducerRecord) {
        xRecord.copyHeadersFrom((ExtensibleProducerRecord) producerRecord);
      }

      Collection<ExtensibleProducerRecord<byte[], byte[]>> xRecords;
      if (useLargeMessageProcessing) {
        xRecords = _messageSplitter.split(xRecord);
      } else {
        xRecords = Collections.singleton(xRecord);
      }

      _auditor.record(auditToken, topic, timestamp, 1L, (long) sizeInBytes, AuditType.ATTEMPT);

      Callback errorLoggingCallback =
        new ErrorLoggingCallback<>(auditToken, topic, timestamp, sizeInBytes, _auditor, callback);

      if (useLargeMessageProcessing) {
        errorLoggingCallback = new LargeMessageCallback(xRecords.size(), errorLoggingCallback);
      }
      Future<RecordMetadata> future = null;
      for (ExtensibleProducerRecord<byte[], byte[]> segmentRecord : xRecords) {
        ProducerRecord<byte[], byte[]> segmentProducerRecord = serializeWithHeaders(segmentRecord, _headerSerializer);
        future = _producer.send(segmentProducerRecord, errorLoggingCallback);
      }
      return future;
    } catch (Exception t) {
      _auditor.record(_auditor.auditToken(producerRecord.key(), producerRecord.value()), producerRecord.topic(),
          producerRecord.timestamp(), 1L, 0L, AuditType.FAILURE);
      throw new KafkaException(t);
    } finally {
      _numThreadsInSend.decrementAndGet();
    }
  }

  private Future<RecordMetadata> sendNullValue(ProducerRecord<K, V> producerRecord, Callback callback, String topic,
    K key, V value, Long timestamp, byte[] serializedValue, byte[] serializedKey, int sizeInBytes, Object auditToken) {
    Future<RecordMetadata> future; // We wrap the user callback for error logging and auditing purpose.
    Callback errorLoggingCallback =
      new ErrorLoggingCallback<>(auditToken, topic, timestamp, sizeInBytes, _auditor, callback);
    _auditor.record(auditToken, topic, timestamp, 1L, (long) sizeInBytes, AuditType.ATTEMPT);
    ProducerRecord<byte[], byte[]> headerlessByteRecord =
      new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp(),
        serializedKey, serializedValue);
    future = _producer.send(headerlessByteRecord, errorLoggingCallback);
    return future;
  }

  /**
   * This is public for testing.
   * This assumes that the value is not a large value that would be segmented by large message support.
   */
  public static ProducerRecord<byte[], byte[]> serializeWithHeaders(ExtensibleProducerRecord<byte[], byte[]> xRecord,
    HeaderSerializer headerParser) {

    if (!xRecord.hasHeaders()) {
      return new ProducerRecord<byte[], byte[]>(xRecord.topic(), xRecord.partition(), xRecord.timestamp(), xRecord.key(), xRecord.value());
    }

    int headersSize = headerParser.serializedHeaderSize(xRecord.headers());
    int serializedValueSize = (xRecord.value() == null ? 0 : xRecord.value().length) + headersSize;

    ByteBuffer valueWithHeaders = ByteBuffer.allocate(serializedValueSize);
    headerParser.serializeHeader(valueWithHeaders, xRecord.headers(), xRecord.value() == null);
    if (xRecord.value() != null) {
      valueWithHeaders.put(xRecord.value());
    }

    if (valueWithHeaders.hasRemaining()) {
      throw new IllegalStateException("Detected slack when writing headers to byte buffer.");
    }

    return new ProducerRecord<>(xRecord.topic(), xRecord.partition(), xRecord.timestamp(), xRecord.key(), valueWithHeaders.array());
  }


  /**
   * This method will flush all the message buffered in producer. It is a blocking call.
   */
  @Override
  public void flush() {
    _producer.flush();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return _producer.partitionsFor(topic);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return _producer.metrics();
  }

  @Override
  public void close() {
    LOG.info("Shutting down LiKafkaProducer...");
    prepClose();
    _auditor.close();
    _producer.close();
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
    long startMs = System.currentTimeMillis();
    LOG.info("Shutting down LiKafkaProducer...");
    _auditor.close(timeout, timeUnit);
    _producer.close(Math.max(0, startMs + timeout - System.currentTimeMillis()), timeUnit);
  }

  private void prepClose() {
    _closed = true;
    // We flush first to avoid the tight loop when buffer is full.
    _producer.flush();
    while (_numThreadsInSend.get() > 0) { }
    _producer.flush();
  }

  private static class ErrorLoggingCallback<K, V> implements Callback {
    private final String _topic;
    private final Long _timestamp;
    private final Integer _serializedSize;
    private final Object _auditToken;
    private final Auditor<K, V> _auditor;
    private final Callback _userCallback;

    public ErrorLoggingCallback(Object auditToken,
                                String topic,
                                Long timestamp,
                                Integer serializedSize,
                                Auditor<K, V> auditor,
                                Callback userCallback) {
      _topic = topic;
      _timestamp = timestamp;
      _serializedSize = serializedSize;
      _auditor = auditor;
      _auditToken = auditToken;
      _userCallback = userCallback;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        LOG.error(String.format("Unable to send record %sto kafka topic %s",
                                _auditToken == null ? "[No Custom Info]" : _auditToken,
                                 _topic), e);
        // Audit the failure.
        _auditor.record(_auditToken, _topic, _timestamp, 1L, _serializedSize.longValue(), AuditType.FAILURE);
      } else {
        // Audit the success.
        _auditor.record(_auditToken, _topic, _timestamp, 1L, _serializedSize.longValue(), AuditType.SUCCESS);
      }
      if (_userCallback != null) {
        _userCallback.onCompletion(recordMetadata, e);
      }
    }
  }

}
