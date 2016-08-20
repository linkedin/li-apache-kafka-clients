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
import com.linkedin.kafka.clients.largemessage.LargeMessageCallback;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import com.linkedin.kafka.clients.largemessage.MessageSplitter;
import com.linkedin.kafka.clients.largemessage.MessageSplitterImpl;
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

/**
 * This producer is the implementation of {@link LiKafkaProducer}.
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

    // Instantiate the open source producer, which always sents raw bytes.
    _producer = new KafkaProducer<>(configs.originals(), new ByteArraySerializer(), new ByteArraySerializer());

    // Instantiate auditor if necessary
    _auditor = auditor != null ? auditor :
        configs.getConfiguredInstance(LiKafkaProducerConfig.AUDITOR_CLASS_CONFIG, Auditor.class);
    _auditor.configure(configs.configsWithCurrentProducer(_producer));
    _auditor.start();
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
    return send(producerRecord, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
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
      _auditor.record(topic, key, value, timestamp, 0, AuditType.ATTEMPT);
      _auditor.record(topic, key, value, timestamp, 0, AuditType.FAILURE);
      throw new KafkaException(t);
    }
    int sizeInBytes = (serializedKey == null ? 0 : serializedKey.length)
        + (serializedValue == null ? 0 : serializedValue.length);
    // Audit the attempt.
    _auditor.record(topic, key, value, timestamp, sizeInBytes, AuditType.ATTEMPT);
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
  public UUID getUuid(K key, V value) {
    return UUID.randomUUID();
  }

  @Override
  public void close() {
    LOG.info("Shutting down LiKafkaProducer...");
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
        _auditor.record(_topic, _key, _value, _timestamp, _serializedSize, AuditType.FAILURE);
      }
      if (_userCallback != null) {
        _userCallback.onCompletion(recordMetadata, e);
      }
      // Audit the success.
      _auditor.record(_topic, _key, _value, _timestamp, _serializedSize, AuditType.SUCCESS);
    }
  }

}
