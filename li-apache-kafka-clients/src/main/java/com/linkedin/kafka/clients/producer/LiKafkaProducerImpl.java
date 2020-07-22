/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;
import com.linkedin.kafka.clients.auditing.NoOpAuditor;
import com.linkedin.kafka.clients.common.EncryptionHeaderValue;
import com.linkedin.kafka.clients.largemessage.LargeMessageCallback;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import com.linkedin.kafka.clients.largemessage.MessageSplitter;
import com.linkedin.kafka.clients.largemessage.MessageSplitterImpl;
import com.linkedin.kafka.clients.largemessage.errors.SkippableException;
import com.linkedin.kafka.clients.security.TopicEncrypterDecrypterManager;
import com.linkedin.kafka.clients.utils.Constants;
import com.linkedin.kafka.clients.utils.PrimitiveEncoderDecoder;
import java.lang.reflect.Field;
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
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.linkedin.kafka.clients.common.LiKafkaCommonClientConfigs.TOPIC_ENCRYPTION_MANAGER_CLASS_CONFIG;


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
  private final boolean _enableHeaderTimestamps;
  private final int _maxMessageSegmentSize;
  private final MessageSplitter _messageSplitter;
  private final boolean _largeMessageSegmentWrappingRequired;
  private final boolean _encryptionEnabled;
  private final TopicEncrypterDecrypterManager _topicEncrypterDecrypterManager;

  // serializers
  private Serializer<K> _keySerializer;
  private Serializer<V> _valueSerializer;

  //custom partitioning
  private Partitioner _partitioner;
  private Method _waitOnMD;
  private Field _mdClusterField;
  private long _maxBlockMs;

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
  private final SerializeStrategy<V> _serializeStrategy;

  public LiKafkaProducerImpl(Properties props) {
    this(new LiKafkaProducerConfig(props), null, null, null, null, null);
  }

  public LiKafkaProducerImpl(Properties props, Serializer<K> keySerializer, Serializer<V> valueSerializer,
      Serializer<LargeMessageSegment> largeMessageSegmentSerializer, Auditor<K, V> auditor) {
    this(new LiKafkaProducerConfig(props), keySerializer, valueSerializer, largeMessageSegmentSerializer, auditor,
        null);
  }

  public LiKafkaProducerImpl(Map<String, ?> configs) {
    this(new LiKafkaProducerConfig(configs), null, null, null, null, null);
  }

  public LiKafkaProducerImpl(Map<String, ?> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer,
      Serializer<LargeMessageSegment> largeMessageSegmentSerializer, Auditor<K, V> auditor) {
    this(new LiKafkaProducerConfig(configs), keySerializer, valueSerializer, largeMessageSegmentSerializer, auditor,
        null);
  }

  public LiKafkaProducerImpl(Map<String, ?> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer,
      Serializer<LargeMessageSegment> largeMessageSegmentSerializer, Auditor<K, V> auditor,
      TopicEncrypterDecrypterManager topicEncrypterDecrypterManager) {
    this(new LiKafkaProducerConfig(configs), keySerializer, valueSerializer, largeMessageSegmentSerializer, auditor,
        topicEncrypterDecrypterManager);
  }

  @SuppressWarnings("unchecked")
  private LiKafkaProducerImpl(LiKafkaProducerConfig configs, Serializer<K> keySerializer, Serializer<V> valueSerializer,
      Serializer<LargeMessageSegment> largeMessageSegmentSerializer, Auditor<K, V> auditor,
      TopicEncrypterDecrypterManager topicEncrypterDecrypterManager) {
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
    _enableHeaderTimestamps = configs.getBoolean(LiKafkaProducerConfig.HEADER_TIMESTAMP_ENABLED);
    try {
      // Instantiate the key serializer if necessary.
      _keySerializer = keySerializer != null ? keySerializer
          : configs.getConfiguredInstance(LiKafkaProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
      _keySerializer.configure(configs.originals(), true);
      // Instantiate the key serializer if necessary.
      _valueSerializer = valueSerializer != null ? valueSerializer
          : configs.getConfiguredInstance(LiKafkaProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
      _valueSerializer.configure(configs.originals(), false);

      // Performance optimization to avoid a call to instanceof for every deserialization call
      if (_valueSerializer instanceof ExtendedSerializer) {
        _serializeStrategy = new SerializeStrategy<V>() {
          @Override
          public byte[] serialize(String topic, Headers headers, V data) {
            return ((ExtendedSerializer<V>) _valueSerializer).serialize(topic, headers, data);
          }
        };
      } else {
        _serializeStrategy = new SerializeStrategy<V>() {
          @Override
          public byte[] serialize(String topic, Headers headers, V data) {
            return _valueSerializer.serialize(topic, data);
          }
        };
      }

      _partitioner = configs.getConfiguredInstance(LiKafkaProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
      _maxBlockMs = configs.getLong(LiKafkaProducerConfig.MAX_BLOCK_MS_CONFIG);

      // prepare to handle large messages.
      _largeMessageEnabled = configs.getBoolean(LiKafkaProducerConfig.LARGE_MESSAGE_ENABLED_CONFIG);

      _encryptionEnabled = configs.getBoolean(LiKafkaProducerConfig.ENCRYPTION_ENABLED_CONFIG);
      if (_encryptionEnabled) {
        if (topicEncrypterDecrypterManager == null) {
          LOG.warn("Cannot detect customized {}. The default one will be used.",
              TopicEncrypterDecrypterManager.class.getSimpleName());
          _topicEncrypterDecrypterManager = configs.getConfiguredInstance(TOPIC_ENCRYPTION_MANAGER_CLASS_CONFIG,
              TopicEncrypterDecrypterManager.class);
        } else {
          _topicEncrypterDecrypterManager = topicEncrypterDecrypterManager;
        }
      } else {
        _topicEncrypterDecrypterManager = null;
      }

      if (_largeMessageEnabled && _partitioner != null) {
        //if large msg support is enabled and the user has provided a custom partitioner (our default for partitioner is null)
        //we will sometimes need to invoke this partitioner ourselves. to do so we would need to get cluster metadata
        //which is only accessible using a private method on the underlying producer.
        try {
          _waitOnMD = _producer.getClass().getDeclaredMethod("waitOnMetadata", String.class, Integer.class, long.class);
          _waitOnMD.setAccessible(true);
          _mdClusterField = Class.forName(KafkaProducer.class.getCanonicalName() + "$ClusterAndWaitTime").getDeclaredField("cluster");
          _mdClusterField.setAccessible(true);
        } catch (NoSuchMethodException | NoSuchFieldException | ClassNotFoundException e) {
          throw new IllegalStateException("custom partitioner specified but cannot get access to waitOnMetadata method", e);
        }
      }

      _maxMessageSegmentSize = Math.min(configs.getInt(LiKafkaProducerConfig.MAX_MESSAGE_SEGMENT_BYTES_CONFIG),
          configs.getInt(LiKafkaProducerConfig.MAX_REQUEST_SIZE_CONFIG));
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

      Headers headers = producerRecord.headers();
      if (_enableHeaderTimestamps) {
        if (headers == null) {
          headers = new RecordHeaders();
        }
        // Remove any header that maybe using the key for audit event timestamp or large message.
        headers.remove(Constants.TIMESTAMP_HEADER);
        headers.add(Constants.TIMESTAMP_HEADER, PrimitiveEncoderDecoder.encodeLong(timestamp));
      }

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
        // ExtendedSerializer is used to test Share dictionary compression
        serializedValue = _serializeStrategy.serialize(topic, headers, value);
        serializedKey = _keySerializer.serialize(topic, key);
      } catch (Throwable t) {
        // Audit the attempt and the failure.
        _auditor.record(auditToken, topic, timestamp, 1L, 0L, AuditType.ATTEMPT);
        throw t;
      }
      // encrypt serialized value; it is possible that the length of encrypted value is different from the original serialized value
      if (_encryptionEnabled) {
        if (serializedValue != null) {
          if (EncryptionHeaderValue.CURRENT_VERSION != EncryptionHeaderValue.V1) {
            serializedValue = _topicEncrypterDecrypterManager.getEncrypterDecrypter(topic).encrypt(serializedValue);
          }
          // the value of ENCRYPTION_HEADER can be used for specific encryption method; in our default encryption implementation, this field is not used here
          EncryptionHeaderValue encryptionHeaderValue = new EncryptionHeaderValue(EncryptionHeaderValue.CURRENT_VERSION);
          headers.add(Constants.ENCRYPTION_HEADER, EncryptionHeaderValue.toBytes(encryptionHeaderValue));
        }
      }

      int serializedKeyLength = serializedKey == null ? 0 : serializedKey.length;
      int serializedValueLength = serializedValue == null ? 0 : serializedValue.length;
      int sizeInBytes = serializedKeyLength + serializedValueLength;
      // Audit the attempt.
      _auditor.record(auditToken, topic, timestamp, 1L, (long) sizeInBytes, AuditType.ATTEMPT);
      // We wrap the user callback for error logging and auditing purpose.
      Callback errorLoggingCallback = new ErrorLoggingCallback<>(messageId, auditToken, topic, timestamp, sizeInBytes, _auditor, callback);

      if (_largeMessageEnabled) {
        if (serializedValueLength > _maxMessageSegmentSize) {
          //payload requires splitting

          //if user didnt explicitely specify a destination partition, and is using a custom partitioner we
          //call the custom partitioner at this point so its decision could apply to all segments (otherwise we
          //risk the custom partitioner sending segments across multiple partitions). we know the kafka default
          //partitioner is "safe" for this (as it only operates on the key, and all segments get the same key)
          if (partition == null && _partitioner != null) {
            partition = invokeCustomPartitioner(topic, key, serializedKey, value, serializedValue);
          }

          // Split the payload into large message segments (they will all have the same key and same partition)
          List<ProducerRecord<byte[], byte[]>> segmentRecords =
              _messageSplitter.split(topic, partition, timestamp, messageId, serializedKey, serializedValue, headers);

          Callback largeMessageCallback = new LargeMessageCallback(segmentRecords.size(), errorLoggingCallback);
          for (ProducerRecord<byte[], byte[]> segmentRecord : segmentRecords) {
            //TODO - there is likely a bug here - we return the future for the last enqueued segment, but they could get out of order...
            future = _producer.send(segmentRecord, largeMessageCallback);
          }

        } else if (_largeMessageSegmentWrappingRequired) {
          //payload does not require splitting, but we wrap anyway

          //if the user didnt specify a partition, and is running a custom partitioner, we invoke it here. this is done
          //to prevent any custom partitioner code from seeing any large msg "envelope" bytes in serialized K and V.
          //we dont need to do this with the default kafka partitioner since it only looks at key (which we dont modify
          //for large msg support)
          if (partition == null && _partitioner != null) {
            partition = invokeCustomPartitioner(topic, key, serializedKey, value, serializedValue);
          }

          // Wrap the paylod with a large message segment, even if the payload is not big enough to split
          List<ProducerRecord<byte[], byte[]>> wrappedRecord =
              _messageSplitter.split(topic, partition, timestamp, messageId, serializedKey, serializedValue,
                  serializedValueLength, headers);
          if (wrappedRecord.size() != 1) {
            throw new IllegalStateException("Failed to create a large message segment wrapped message");
          }
          future = _producer.send(wrappedRecord.get(0), errorLoggingCallback);

        } else {
          // Do not wrap with a large message segment (any partitioner it will be called by the underlying producer)
          future = _producer.send(new ProducerRecord<>(topic, partition, timestamp, serializedKey, serializedValue, headers), errorLoggingCallback);
        }
      } else {
        // Do not wrap with a large message segment (any custom partitioner it will be called by the underlying producer)
        future = _producer.send(new ProducerRecord<>(topic, partition, timestamp, serializedKey, serializedValue, headers), errorLoggingCallback);
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
        throw new IllegalStateException("failed to invoke the bounded flush method", e);
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

    //wait for all producing threads to clear the auditor
    //if there's a meaningful auditor.
    if (!(_auditor instanceof NoOpAuditor)) {
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
    }
    _auditor.close(Math.max(0, deadlineTimeMs - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    _producer.close(Math.max(0, deadlineTimeMs - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    LOG.info("LiKafkaProducer shutdown complete in {} millis", (System.currentTimeMillis() - startTimeMs));
  }

  protected int invokeCustomPartitioner(String topic, K key, byte[] serializedKey, V value, byte[] serializedValue) {
    Cluster cluster;
    try {
      Object md = _waitOnMD.invoke(_producer, topic, null, _maxBlockMs);
      if (md == null) {
        throw new IllegalStateException("couldnt get metadata for topic " + topic);
      }
      cluster = (Cluster) _mdClusterField.get(md);
      if (cluster == null) {
        throw new IllegalStateException("couldnt get metadata for topic " + topic);
      }
    } catch (Exception e) {
      throw new IllegalStateException("while fetching metadata for topic " + topic, e);
    }
    return  _partitioner.partition(topic, key, serializedKey, value, serializedValue, cluster);
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

  private static interface SerializeStrategy<V> {
    byte[] serialize(String topic, Headers headers, V data);
  }
}
