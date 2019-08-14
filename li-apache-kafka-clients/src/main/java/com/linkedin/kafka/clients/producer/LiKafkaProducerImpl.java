/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.largemessage.LargeMessageCallback;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import com.linkedin.kafka.clients.largemessage.MessageSplitter;
import com.linkedin.kafka.clients.largemessage.MessageSplitterImpl;
import com.linkedin.kafka.clients.largemessage.errors.SkippableException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * LiKafkaProducer&lt;String, String&gt; liKafkaProducer = new LiKafkaProducerImpl&lt;&gt;(props);
 * for(int i = 0; i &lt; 100; i++)
 *     liKafkaProducer.send(new ProducerRecord&lt;String, String$gt;("my-topic", Integer.toString(i), Integer.toString(i)));
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
 * to the auditor when invoking {@link Auditor#configure(java.util.Map)}, but only if you configure
 * {@link LiKafkaProducerConfig#AUDITOR_CLASS_CONFIG} in producer properties. The auditor implementation can get that
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
 * If you pass {@link Auditor} as a constructor argument, you may want to invoke {@link Auditor#configure(java.util.Map)}
 * explicitly in your code before passing it into {@link LiKafkaProducerImpl} constructor.
 * If the underlying KafkaProducer is shared by the auditor implementation. The auditor should not close the shared
 * vanilla producer when {@link Auditor#close()} is invoked.
 */
public class LiKafkaProducerImpl<K, V> implements LiKafkaProducer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaProducerImpl.class);
  private static final String BOUNDED_FLUSH_THREAD_PREFIX = "Bounded-Flush-Thread-";

  // Large message settings
  private final boolean _largeMessageEnabled;
  private final int _maxMessageSegmentSize;
  private final MessageSplitter _messageSplitter;
  private final boolean _largeMessageSegmentWrappingRequired;

  // serializers
  private Serializer<K> _keySerializer;
  private Serializer<V> _valueSerializer;

  // raw byte producer
  protected final Producer<byte[], byte[]> _producer;
  /*package private for testing*/ Auditor<K, V> _auditor;
  private final UUIDFactory<K, V> _uuidFactory;

  // A counter of the threads in the middle of sending messages. This is needed to ensure when we close the producer
  // everything is audited.
  private final AtomicInteger _numThreadsInSend;
  private volatile boolean _closed;

  // This is null if the underlying producer does not have an implementation for time-bounded flush
  private final Method _boundedFlushMethod;
  private final AtomicInteger _boundFlushThreadCount = new AtomicInteger();

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
    // TODO: This hack remains until bounded flush is added to apache/kafka
    Method producerSupportsBoundedFlush;
    try {
      producerSupportsBoundedFlush = _producer.getClass().getMethod("flush", long.class, TimeUnit.class);
    } catch (NoSuchMethodException e) {
      LOG.warn("Wrapped KafkaProducer does not support time-bounded flush.", e);
      producerSupportsBoundedFlush = null;
    }
    _boundedFlushMethod = producerSupportsBoundedFlush;

    try {
      // Instantiate the key serializer if necessary.
      _keySerializer = keySerializer != null ? keySerializer
          : configs.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
      _keySerializer.configure(configs.originals(), true);
      // Instantiate the key serializer if necessary.
      _valueSerializer = valueSerializer != null ? valueSerializer
          : configs.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
      _valueSerializer.configure(configs.originals(), false);

      // prepare to handle large messages.
      _largeMessageEnabled = configs.getBoolean(LiKafkaProducerConfig.LARGE_MESSAGE_ENABLED_CONFIG);
      _maxMessageSegmentSize = Math.min(configs.getInt(LiKafkaProducerConfig.MAX_MESSAGE_SEGMENT_BYTES_CONFIG),
          configs.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
      Serializer<LargeMessageSegment> segmentSerializer = largeMessageSegmentSerializer != null ? largeMessageSegmentSerializer
          : configs.getConfiguredInstance(LiKafkaProducerConfig.SEGMENT_SERIALIZER_CLASS_CONFIG, Serializer.class);
      segmentSerializer.configure(configs.originals(), false);
      _uuidFactory = configs.getConfiguredInstance(LiKafkaProducerConfig.UUID_FACTORY_CLASS_CONFIG, UUIDFactory.class);
      _messageSplitter = new MessageSplitterImpl(_maxMessageSegmentSize, segmentSerializer, _uuidFactory);
      _largeMessageSegmentWrappingRequired =
          configs.getBoolean(LiKafkaProducerConfig.LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_CONFIG);

      // Instantiate auditor if necessary
      if (auditor != null) {
        _auditor = auditor;
        _auditor.configure(configs.configsWithCurrentProducer(_producer));
      } else {
        _auditor = configs.getConfiguredInstance(LiKafkaProducerConfig.AUDITOR_CLASS_CONFIG, Auditor.class, _producer);
      }
      _auditor.start();
      _numThreadsInSend = new AtomicInteger(0);
      _closed = false;
    } catch (Exception e) {
      _producer.close();
      throw e;
    }
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
    return send(producerRecord, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
    _numThreadsInSend.incrementAndGet();
    boolean failed = true;
    try {
      if (_closed) {
        throw new IllegalStateException("LiKafkaProducer has been closed.");
      }
      String topic = producerRecord.topic();
      K key = producerRecord.key();
      V value = producerRecord.value();
      Object auditToken = _auditor.auditToken(key, value);
      Long timestamp = producerRecord.timestamp() == null ? System.currentTimeMillis() : producerRecord.timestamp();
      Integer partition = producerRecord.partition();
      Future<RecordMetadata> future = null;
      UUID messageId = _uuidFactory.getUuid(producerRecord);
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
        _auditor.record(auditToken, topic, timestamp, 1L, 0L, AuditType.ATTEMPT);
        throw t;
      }
      int serializedKeyLength = serializedKey == null ? 0 : serializedKey.length;
      int serializedValueLength = serializedValue == null ? 0 : serializedValue.length;
      int sizeInBytes = serializedKeyLength + serializedValueLength;
      // Audit the attempt.
      _auditor.record(auditToken, topic, timestamp, 1L, (long) sizeInBytes, AuditType.ATTEMPT);
      // We wrap the user callback for error logging and auditing purpose.
      Callback errorLoggingCallback =
          new ErrorLoggingCallback<>(messageId, auditToken, topic, timestamp, sizeInBytes, _auditor, callback);
      if (_largeMessageEnabled && serializedValueLength > _maxMessageSegmentSize) {
        // Split the payload into large message segments
        List<ProducerRecord<byte[], byte[]>> segmentRecords =
            _messageSplitter.split(topic, partition, timestamp, messageId, serializedKey, serializedValue);
        Callback largeMessageCallback = new LargeMessageCallback(segmentRecords.size(), errorLoggingCallback);
        for (ProducerRecord<byte[], byte[]> segmentRecord : segmentRecords) {
          future = _producer.send(segmentRecord, largeMessageCallback);
        }
      } else if (_largeMessageEnabled && _largeMessageSegmentWrappingRequired) {
        // Wrap the paylod with a large message segment, even if the payload is not big enough to split
        List<ProducerRecord<byte[], byte[]>> wrappedRecord =
            _messageSplitter.split(topic, partition, timestamp, messageId, serializedKey, serializedValue,
                serializedValueLength);
        if (wrappedRecord.size() != 1) {
          throw new IllegalStateException("Failed to create a large message segment wrapped message");
        }
        future = _producer.send(wrappedRecord.get(0), errorLoggingCallback);
      } else {
        // Do not wrap with a large message segment
        future = _producer.send(new ProducerRecord(topic, partition, timestamp, serializedKey, serializedValue),
            errorLoggingCallback);
      }
      failed = false;
      return future;
    } catch (SkippableException e) {
      LOG.warn("Exception thrown when producing message to partition {}-{}", producerRecord.topic(), producerRecord.partition());
      return null;
    } finally {
      if (failed) {
        _auditor.record(_auditor.auditToken(producerRecord.key(), producerRecord.value()), producerRecord.topic(),
                        producerRecord.timestamp(), 1L, 0L, AuditType.FAILURE);
      }
      _numThreadsInSend.decrementAndGet();
      if (_closed) {
        synchronized (_numThreadsInSend) {
          _numThreadsInSend.notifyAll();
        }
      }
    }
  }

  /**
   * This method will flush all the message buffered in producer. It is a blocking call.
   */
  @Override
  public void flush() {
    _producer.flush();
  }

  /**
   * This method will flush all the message buffered in producer. The call blocks until timeout.
   * If the underlying producer doesn't support a bounded flush, it will invoke the {@link #flush()}.
   */
  @Override
  public void flush(long timeout, TimeUnit timeUnit) {
    boolean useSeparateThreadForFlush = false;
    if (_boundedFlushMethod != null) {
      try {
        _boundedFlushMethod.invoke(_producer, timeout, timeUnit);
      } catch (IllegalAccessException | InvocationTargetException e) {
        LOG.trace("Underlying producer does not support bounded flush!", e);
        useSeparateThreadForFlush = true;
      }
    } else {
      useSeparateThreadForFlush = true;
    }

    if (useSeparateThreadForFlush) {
      final CountDownLatch latch = new CountDownLatch(1);
      Thread t = new Thread(() -> {
        _producer.flush();
        latch.countDown();
      });
      t.setDaemon(true);
      t.setName(BOUNDED_FLUSH_THREAD_PREFIX + _boundFlushThreadCount.getAndIncrement());
      t.setUncaughtExceptionHandler((t1, e) -> {
        LOG.warn("Thread " + t1.getName() + " terminated unexpectedly.", e);
      });
      t.start();

      boolean latchResult = false;
      try {
        latchResult = latch.await(timeout, timeUnit);
      } catch (InterruptedException e) {
        throw new InterruptException("Flush interruped.", e);
      }
      if (!latchResult) {
        throw new TimeoutException("Failed to flush accumulated records within " + timeout + " " + timeUnit);
      }
    }
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
    close(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
    LOG.info("Shutting down LiKafkaProducer in {} {}...", timeout, timeUnit);
    long startTimeMs = System.currentTimeMillis();
    long budgetMs = timeUnit.toMillis(timeout);
    long deadlineTimeMs = startTimeMs + budgetMs;

    _closed = true;
    synchronized (_numThreadsInSend) {
      long remainingMs = deadlineTimeMs - System.currentTimeMillis();
      while (_numThreadsInSend.get() > 0 && remainingMs > 0) {
        try {
          _numThreadsInSend.wait(remainingMs);
        } catch (InterruptedException e) {
          LOG.error("Interrupted when there are still {} sender threads.", _numThreadsInSend.get());
          break;
        }
        remainingMs = deadlineTimeMs - System.currentTimeMillis();
      }
    }

    _auditor.close(Math.max(0, deadlineTimeMs - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    _producer.close(Math.max(0, deadlineTimeMs - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    LOG.info("LiKafkaProducer shutdown complete in {} millis", (System.currentTimeMillis() - startTimeMs));
  }

  private static class ErrorLoggingCallback<K, V> implements Callback {
    private final UUID _messageId;
    private final String _topic;
    private final Long _timestamp;
    private final Integer _serializedSize;
    private final Object _auditToken;
    private final Auditor<K, V> _auditor;
    private final Callback _userCallback;

    public ErrorLoggingCallback(UUID messageId,
                                Object auditToken,
                                String topic,
                                Long timestamp,
                                Integer serializedSize,
                                Auditor<K, V> auditor,
                                Callback userCallback) {
      _messageId = messageId;
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
        LOG.error(String.format("Unable to send event %s with message id %s to kafka topic %s",
                                _auditToken == null ? "[No Custom Info]" : _auditToken,
                                (_messageId != null) ? _messageId.toString().replaceAll("-", "") : "[none]", _topic), e);
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

  @Override
  public void initTransactions() {
    _producer.initTransactions();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    _producer.beginTransaction();
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                       String consumerGroupId) throws ProducerFencedException {
    _producer.sendOffsetsToTransaction(offsets, consumerGroupId);
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    _producer.commitTransaction();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    _producer.abortTransaction();
  }
}
