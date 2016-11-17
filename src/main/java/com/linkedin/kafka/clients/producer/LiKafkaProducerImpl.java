/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.consumer.HeaderKeySpace;
import com.linkedin.kafka.clients.largemessage.LargeMessageCallback;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import com.linkedin.kafka.clients.largemessage.MessageSplitter;
import com.linkedin.kafka.clients.largemessage.MessageSplitterImpl;
import com.linkedin.kafka.clients.utils.HeaderParser;
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
import java.util.UUID;
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
 * multiple {@link LargeMessageSegment} and sent to Kafka brokers as individual messages. On the consumer side,
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
 * props.put("segment.serializer", DefaultSegmentSerializer.class.getName());
 * props.put("auditor.class", LoggingAuditor.class.getName());
 *
 * LiKafkaProducer<String, String> liKafkaProducer = new LiKafkaProducerImpl<>(props);
 * for(int i = 0; i < 100; i++)
 *     liKafkaProducer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * </code></pre>
 * The segment serializer will be used to serialize the {@link LargeMessageSegment}. The users may have different
 * serialization/deserialization protocol for the segments of large messages. If no segment serializer is
 * specified by the user, the {@link com.linkedin.kafka.clients.largemessage.DefaultSegmentSerializer} will be used.
 * <p>
 * User can also specify an {@link Auditor} implementation class for LiKafkaProducerImpl. For each ProducerRecord
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
 * public void configure(Map<String, ?> configs) {
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
  private volatile boolean _closed;

  public LiKafkaProducerImpl(Properties props) {
    this(new LiKafkaProducerConfig(props), null, null, null, null);
  }

  public LiKafkaProducerImpl(Properties props,
                             Serializer<K> keySerializer,
                             Serializer<V> valueSerializer,
                             Serializer<LargeMessageSegment> largeMessageSegmentSerializer,
                             Auditor<K, V> auditor) {
    this(new LiKafkaProducerConfig(props), keySerializer, valueSerializer, largeMessageSegmentSerializer, auditor);
  }

  public LiKafkaProducerImpl(Map<String, ?> configs) {
    this(new LiKafkaProducerConfig(configs), null, null, null, null);
  }

  public LiKafkaProducerImpl(Map<String, ?> configs,
                             Serializer<K> keySerializer,
                             Serializer<V> valueSerializer,
                             Serializer<LargeMessageSegment> largeMessageSegmentSerializer,
                             Auditor<K, V> auditor) {
    this(new LiKafkaProducerConfig(configs), keySerializer, valueSerializer, largeMessageSegmentSerializer, auditor);
  }

  @SuppressWarnings("unchecked")
  private LiKafkaProducerImpl(LiKafkaProducerConfig configs,
                             Serializer<K> keySerializer,
                             Serializer<V> valueSerializer,
                             Serializer<LargeMessageSegment> largeMessageSegmentSerializer,
                             Auditor<K, V> auditor) {
    // Instantiate the open source producer, which always sents raw bytes.
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
    Serializer<LargeMessageSegment> segmentSerializer = largeMessageSegmentSerializer != null ? largeMessageSegmentSerializer :
        configs.getConfiguredInstance(LiKafkaProducerConfig.SEGMENT_SERIALIZER_CLASS_CONFIG, Serializer.class);
    segmentSerializer.configure(configs.originals(), false);
    _messageSplitter = new MessageSplitterImpl(_maxMessageSegmentSize, segmentSerializer);

    // Instantiate auditor if necessary
    _auditor = auditor != null ? auditor :
        configs.getConfiguredInstance(LiKafkaProducerConfig.AUDITOR_CLASS_CONFIG, Auditor.class);
    _auditor.configure(configs.configsWithCurrentProducer(_producer));
    _auditor.start();
    _numThreadsInSend = new AtomicInteger(0);
    _closed = false;
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
      Long timestamp = producerRecord.timestamp() == null ? System.currentTimeMillis() : producerRecord.timestamp();
      Integer partition = producerRecord.partition();
      Future<RecordMetadata> future = null;
      UUID messageId = getUuid(key, value);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Sending event: [{}, {}] with key {} to kafka topic {}",
            messageId.toString().replaceAll("-", ""),
            value.toString(),
            (key != null) ? key.toString() : "[none]",
            topic);
      }
      byte[] serializedValue;
      byte[] serializedKey;
      try {
        serializedValue = _valueSerializer.serialize(topic, value);
        serializedKey = _keySerializer.serialize(topic, key);
      } catch (Throwable t) {
        // Audit the attempt and the failure.
        _auditor.record(topic, key, value, timestamp, 1L, 0L, AuditType.ATTEMPT);
        _auditor.record(topic, key, value, timestamp, 1L, 0L, AuditType.FAILURE);
        throw new KafkaException(t);
      }
      int sizeInBytes = (serializedKey == null ? 0 : serializedKey.length)
          + (serializedValue == null ? 0 : serializedValue.length);
      // Audit the attempt.
      _auditor.record(topic, key, value, timestamp, 1L, (long) sizeInBytes, AuditType.ATTEMPT);
      // We wrap the user callback for error logging and auditing purpose.
      Callback errorLoggingCallback =
          new ErrorLoggingCallback<>(messageId, key, value, topic, timestamp, sizeInBytes, _auditor, callback);
      if (_largeMessageEnabled && serializedValue != null && serializedValue.length > _maxMessageSegmentSize) {
        List<ProducerRecord<byte[], byte[]>> segmentRecords =
            _messageSplitter.split(topic, partition, messageId, serializedKey, serializedValue);
        Callback largeMessageCallback = new LargeMessageCallback(segmentRecords.size(), errorLoggingCallback);
        for (ProducerRecord<byte[], byte[]> segmentRecord : segmentRecords) {
          future = _producer.send(segmentRecord, largeMessageCallback);
        }
      } else {
        // In order to make sure consumer can consume both large message segment and the ordinary message,
        // we wrap the normal message as a single segment large message. When consumer sees it, it will
        // be returned by message assembler immediately. We set a pretty large maxSegmentSize to make sure
        // the message will end up in one segment.
        List<ProducerRecord<byte[], byte[]>> wrappedRecord =
            _messageSplitter.split(topic, partition, timestamp, messageId, serializedKey, serializedValue, Integer.MAX_VALUE / 2);
        assert (wrappedRecord.size() == 1);
        future = _producer.send(wrappedRecord.get(0), errorLoggingCallback);
      }
      return future;
    } catch (Throwable t) {
      _auditor.record(producerRecord.topic(), producerRecord.key(), producerRecord.value(), producerRecord.timestamp(),
          1L, 0L, AuditType.FAILURE);
      throw new KafkaException(t);
    } finally {
      _numThreadsInSend.decrementAndGet();
    }
  }

  /**
   * This sends a record value that looks like:
   * <pre>
   * magic | user record value length | user record value bytes | header size in bytes | header 0 size | header 0 bytes | ...
   * </pre>
   *
   * @param producerRecord
   * @param callback
   * @return
   */
  private Future<RecordMetadata> sendX(ProducerRecord<K, V> producerRecord, Callback callback) {
    _numThreadsInSend.incrementAndGet();
    try {
      if (_closed) {
        throw new IllegalStateException("LiKafkaProducer has been closed.");
      }
      String topic = producerRecord.topic();
      K key = producerRecord.key();
      V value = producerRecord.value();
      Long timestamp = producerRecord.timestamp() == null ? System.currentTimeMillis() : producerRecord.timestamp();
      Integer partition = producerRecord.partition();
      Future<RecordMetadata> future = null;
      UUID messageId = getUuid(key, value);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Sending event: [{}, {}] with key {} to kafka topic {}", messageId.toString().replaceAll("-", ""),
          value.toString(), (key != null) ? key.toString() : "[none]", topic);
      }
      byte[] serializedValue;
      byte[] serializedKey;
      Integer auditSize;
      try {
        serializedKey = _keySerializer.serialize(topic, key);
        byte[] encapsulatedValue = _valueSerializer.serialize(topic, value);
        int encapsulatedValueSize = (encapsulatedValue == null) ? 0 : encapsulatedValue.length + 4 + 4; /* header size */
        int headerSize = HeaderParser.serializedHeaderSize(producerRecord.headers());
        int magicSize = (producerRecord.headers() == null && encapsulatedValue == null) ? 0 : 4;
        ByteBuffer outputBuf = ByteBuffer.allocate(magicSize + encapsulatedValueSize + headerSize);
        if (magicSize > 0) {
          outputBuf.putInt(HeaderParser.HEADER_VALUE_MAGIC);
        }
        if (encapsulatedValue != null) {
          outputBuf.putInt(HeaderKeySpace.PAYLOAD_HEADER_KEY);
          outputBuf.putInt(encapsulatedValue.length);
          outputBuf.put(encapsulatedValue);
        }
        HeaderParser.writeHeader(outputBuf, producerRecord.headers());
        serializedValue = outputBuf.array();
        auditSize = (serializedKey == null ? 0 : serializedKey.length) + serializedValue.length;
        //TODO: At some point we may want to extend the audit mechanism to count all the header and overhead bytes
        _auditor.record(topic, key, value, timestamp, 1L, auditSize.longValue(),  AuditType.ATTEMPT);
      } catch (Throwable t) {
        // Audit the attempt and the failure.
        _auditor.record(topic, key, value, timestamp, 1L, 0L, AuditType.ATTEMPT);
        _auditor.record(topic, key, value, timestamp, 1L, 0L, AuditType.FAILURE);
        throw new KafkaException(t);
      }

      if (_largeMessageEnabled && serializedValue != null && serializedValue.length > _maxMessageSegmentSize) {
        //TODO: implement me
        throw new UnsupportedOperationException("Sending large extensible messages is not implemented");
      }

      Callback errorLoggingCallback =
        new ErrorLoggingCallback<>(messageId, key, value, topic, timestamp, auditSize, _auditor, callback);

      if (serializedValue.length == 0) {
        serializedValue = null;
      }
      ProducerRecord<byte[], byte[]> recordWithHeaders =
        new ProducerRecord<>(topic, partition, timestamp, serializedKey, serializedValue);
      return _producer.send(recordWithHeaders, errorLoggingCallback);
    } catch (Throwable t) {
      _auditor.record(producerRecord.topic(), producerRecord.key(), producerRecord.value(), producerRecord.timestamp(),
        1L, 0L, AuditType.FAILURE);
      throw new KafkaException(t);
    } finally {
      _numThreadsInSend.decrementAndGet();
    }
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

  protected UUID getUuid(K key, V value) {
    return UUID.randomUUID();
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
    private final UUID _messageId;
    private final K _key;
    private final V _value;
    private final String _topic;
    private final Long _timestamp;
    private final Integer _serializedSize;
    private final Auditor<K, V> _auditor;
    private final Callback _userCallback;

    public ErrorLoggingCallback(UUID messageId,
                                K key,
                                V value,
                                String topic,
                                Long timestamp,
                                Integer serializedSize,
                                Auditor<K, V> auditor,
                                Callback userCallback) {
      _messageId = messageId;
      _value = value;
      _key = key;
      _topic = topic;
      _timestamp = timestamp;
      _serializedSize = serializedSize;
      _auditor = auditor;
      _userCallback = userCallback;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        LOG.error(String.format("Unable to send event %s with key %s and message id %s to kafka topic %s",
            _value.toString(), (_key != null) ? _key : "[none]",
            (_messageId != null) ? _messageId.toString().replaceAll("-", "") : "[none]", _topic), e);
        // Audit the failure.
        _auditor.record(_topic, _key, _value, _timestamp, 1L, _serializedSize.longValue(), AuditType.FAILURE);
      } else {
        // Audit the success.
        _auditor.record(_topic, _key, _value, _timestamp, 1L, _serializedSize.longValue(), AuditType.SUCCESS);
      }
      if (_userCallback != null) {
        _userCallback.onCompletion(recordMetadata, e);
      }
    }
  }

}
