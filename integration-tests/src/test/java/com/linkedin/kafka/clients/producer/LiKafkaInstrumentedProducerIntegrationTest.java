/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.linkedin.kafka.clients.utils.LiKafkaClientsUtils;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import com.linkedin.kafka.clients.utils.tests.KafkaTestUtils;
import com.linkedin.mario.common.models.v1.ClientConfigRule;
import com.linkedin.mario.common.models.v1.ClientConfigRules;
import com.linkedin.mario.common.models.v1.ClientPredicates;
import com.linkedin.mario.common.models.v1.KafkaClusterDescriptor;
import com.linkedin.mario.server.EmbeddableMario;
import com.linkedin.mario.server.config.MarioConfiguration;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LiKafkaInstrumentedProducerIntegrationTest extends AbstractKafkaClientsIntegrationTestHarness {

  @BeforeMethod
  @Override
  public void setUp() {
    super.setUp();
  }

  @AfterMethod
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void testProducerLiveConfigReload() throws Exception {
    String topic = "testProducerLiveConfigReload";
    createTopic(topic, 1);
    MarioConfiguration marioConfiguration = MarioConfiguration.embeddableInMem();
    marioConfiguration.setEnableNgSupport(false);
    EmbeddableMario mario = new EmbeddableMario(marioConfiguration);
    Random random = new Random();

    // register kafka cluster to EmbeddableMario
    KafkaClusterDescriptor kafkaClusterDescriptor = new KafkaClusterDescriptor(
        null,
        0,
        "test",
        "test",
        "test",
        zkConnect(),
        bootstrapServers(),
        "test",
        0L,
        false
    );
    mario.addKafkaCluster(kafkaClusterDescriptor).get();

    Properties extra = new Properties();
    extra.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "" + 1500);
    extra.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    extra.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    extra.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    Properties baseProducerConfig = getProducerProperties(extra);
    LiKafkaInstrumentedProducerImpl<byte[], byte[]> producer = new LiKafkaInstrumentedProducerImpl<>(
        baseProducerConfig,
        Collections.emptyMap(),
        (baseConfig, overrideConfig) -> new LiKafkaProducerImpl<>(LiKafkaClientsUtils.getConsolidatedProperties(baseConfig, overrideConfig)),
        mario::getUrl);

    byte[] key = new byte[500];
    byte[] value = new byte[500];
    random.nextBytes(key);
    random.nextBytes(value);
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, 0, key, value);
    RecordMetadata recordMetadata = producer.send(record).get();

    Producer<byte[], byte[]> delegate = producer.getDelegate();

    key = new byte[3000];
    value = new byte[3000];
    random.nextBytes(key);
    random.nextBytes(value);
    record = new ProducerRecord<>(topic, 0, key, value);
    try {
      producer.send(record).get();
      Assert.fail("record expected to fail");
    } catch (Exception e) {
      Throwable root = Throwables.getRootCause(e);
      Assert.assertTrue(root instanceof RecordTooLargeException, root.getClass() + " is not a RecordTooLargeException");
    }

    //install a new config policy, wait for the push
    mario.setConfigPolicy(new ClientConfigRules(Collections.singletonList(
        new ClientConfigRule(ClientPredicates.ALL, ImmutableMap.of("max.request.size", "" + 9000)))));

    KafkaTestUtils.waitUntil("delegate recreated", () -> {
      Producer<byte[], byte[]> delegateNow = producer.getDelegate();
      return delegateNow != delegate;
    }, 1, 2, TimeUnit.MINUTES, false);

    producer.send(record).get(); //should succeed this time

    producer.close(Duration.ofSeconds(30));
    mario.close();
  }

  @Test
  public void testCloseFromProduceCallbackOnSenderThread() throws Exception {
    String topic = "testCloseFromProduceCallbackOnSenderThread";
    createTopic(topic, 1);

    Random random = new Random(666);
    Properties extra = new Properties();
    extra.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "" + 50000000); //~50MB (larger than broker-size setting)
    extra.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    extra.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    extra.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    Properties baseProducerConfig = getProducerProperties(extra);
    LiKafkaInstrumentedProducerImpl<byte[], byte[]> producer = new LiKafkaInstrumentedProducerImpl<byte[], byte[]>(
        baseProducerConfig,
        Collections.emptyMap(),
        (baseConfig, overrideConfig) -> new LiKafkaProducerImpl<byte[], byte[]>(LiKafkaClientsUtils.getConsolidatedProperties(baseConfig, overrideConfig)),
        () -> "bogus",
        10 //dont wait for a mario connection
    );

    byte[] key = new byte[3000];
    byte[] value = new byte[49000000];
    random.nextBytes(key);
    random.nextBytes(value); //random data is incompressible, making sure our request is large
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);

    AtomicReference<Throwable> issueRef = new AtomicReference<>();
    Thread testThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          final Thread ourThread = Thread.currentThread();
          Future<RecordMetadata> future = producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
              //we expect a RecordTooLargeException. we also expect this to happen
              //on the same thread.
              if (Thread.currentThread() != ourThread) {
                issueRef.compareAndSet(null,
                    new IllegalStateException("completion did not happen on caller thread by " + Thread.currentThread().getName())
                );
              }
              producer.close(1, TimeUnit.SECONDS);
            }
          });
          RecordMetadata recordMetadata = future.get(1, TimeUnit.MINUTES);
        } catch (Throwable anything) {
          issueRef.compareAndSet(null, anything);
        }
      }
    }, "testCloseFromProduceCallbackOnSenderThread-thread");
    testThread.setDaemon(true);
    testThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        issueRef.compareAndSet(null, e);
      }
    });
    testThread.start();

    testThread.join(TimeUnit.MINUTES.toMillis(1));
    Thread.State state = testThread.getState();
    Assert.assertEquals(
        state,
        Thread.State.TERMINATED,
        "thread was expected to finish, instead its " + state
    );
    Throwable issue = issueRef.get();
    Throwable root = Throwables.getRootCause(issue);
    Assert.assertTrue(root instanceof RecordTooLargeException, root.getMessage());
  }

  @Test
  public void testProducerFlushAfterClose() throws Exception {
    String topic = "testCloseFromProduceCallbackOnSenderThread";
    createTopic(topic, 1);

    Random random = new Random(666);
    Properties extra = new Properties();
    extra.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "" + 1500);
    extra.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    extra.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    extra.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    Properties baseProducerConfig = getProducerProperties(extra);
    LiKafkaInstrumentedProducerImpl<byte[], byte[]> producer = new LiKafkaInstrumentedProducerImpl<byte[], byte[]>(
        baseProducerConfig,
        Collections.emptyMap(),
        (baseConfig, overrideConfig) -> new LiKafkaProducerImpl<byte[], byte[]>(LiKafkaClientsUtils.getConsolidatedProperties(baseConfig, overrideConfig)),
        () -> "bogus",
        10
    );
    byte[] key = new byte[500];
    byte[] value = new byte[500];
    random.nextBytes(key);
    random.nextBytes(value);
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);

    producer.send(record);
    producer.close(Duration.ofSeconds(0));
    producer.flush(0, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testProducerFlushWithTimeoutException() throws Exception {
    String topic = "testCloseFromProduceCallbackOnSenderThread";
    createTopic(topic, 1);

    Random random = new Random(666);
    Properties extra = new Properties();
    extra.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "" + 1500);
    extra.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    extra.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    extra.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    Properties baseProducerConfig = getProducerProperties(extra);
    LiKafkaInstrumentedProducerImpl<byte[], byte[]> producer = new LiKafkaInstrumentedProducerImpl<byte[], byte[]>(
        baseProducerConfig,
        Collections.emptyMap(),
        (baseConfig, overrideConfig) -> new LiKafkaProducerImpl<byte[], byte[]>(LiKafkaClientsUtils.getConsolidatedProperties(baseConfig, overrideConfig)),
        () -> "bogus",
        10
    );
    byte[] key = new byte[500];
    byte[] value = new byte[500];
    random.nextBytes(key);
    random.nextBytes(value);
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);
    producer.send(record);
    // kill brokers
    Set<Integer> brokerIds = super._brokers.keySet();
    for (int id: brokerIds) {
      super.killBroker(id);
    }
    producer.send(record);
    Assert.assertThrows(TimeoutException.class, () -> producer.flush(0, TimeUnit.MILLISECONDS));
  }

  private void createTopic(String topicName, int numPartitions) throws Exception {
    try (AdminClient adminClient = createRawAdminClient(null)) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, numPartitions, (short) 1))).all().get(1, TimeUnit.MINUTES);
    }
  }
}
