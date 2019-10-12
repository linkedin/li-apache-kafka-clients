/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils.tests;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;


public class KafkaTestUtils {
  private final static AtomicBoolean SHUTDOWN_HOOK_INSTALLED = new AtomicBoolean(false);
  private final static Thread SHUTDOWN_HOOK;
  private final static List<File> FILES_TO_CLEAN_UP = Collections.synchronizedList(new ArrayList<>());
  public final static String EXCEPTION_MESSAGE = "DESERIALIZATION_EXCEPTION_";

  static {
    SHUTDOWN_HOOK = new Thread(() -> {
      Exception firstIssue = null;
      for (File toCleanUp : FILES_TO_CLEAN_UP) {
        if (!toCleanUp.exists()) {
          continue;
        }
        try {
          FileUtils.forceDelete(toCleanUp);
        } catch (IOException issue) {
          if (firstIssue == null) {
            firstIssue = issue;
          } else {
            firstIssue.addSuppressed(issue);
          }
        }
      }
      if (firstIssue != null) {
        System.err.println("unable to delete one or more files");
        firstIssue.printStackTrace(System.err);
        throw new IllegalStateException(firstIssue);
      }
    }, "KafkaTestUtils cleanup hook");
    SHUTDOWN_HOOK.setUncaughtExceptionHandler((t, e) -> {
      System.err.println("thread " + t.getName() + " died to uncaught exception");
      e.printStackTrace(System.err);
    });
  }

  private KafkaTestUtils() {
    //utility class
  }

  public static KafkaProducer<String, String> vanillaProducerFor(EmbeddedBroker broker) {
    String bootstrap = broker.getPlaintextAddr();
    if (bootstrap == null) {
      bootstrap = broker.getSslAddr();
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrap);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 1024 * 1024);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<>(props);
  }

  public static KafkaConsumer<String, String> vanillaConsumerFor(EmbeddedBroker broker) {
    String bootstrap = broker.getPlaintextAddr();
    if (bootstrap == null) {
      bootstrap = broker.getSslAddr();
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrap);
    props.put("group.id", "test");
    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    return consumer;
  }

  public static AdminClient adminClientFor(EmbeddedBroker broker) {
    String bootstrap = broker.getPlaintextAddr();
    if (bootstrap == null) {
      bootstrap = broker.getSslAddr();
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrap);

    return AdminClient.create(props);
  }

  public static File newTempDir() {
    try {
      return cleanup(Files.createTempDirectory(null).toFile());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static File cleanup(File toCleanUp) {
    if (SHUTDOWN_HOOK_INSTALLED.compareAndSet(false, true)) {
      Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);
    }
    FILES_TO_CLEAN_UP.add(toCleanUp);
    return toCleanUp;
  }

  public static void quietly(Task task) {
    try {
      task.run();
    } catch (Exception e) {
      e.printStackTrace(System.err);
    }
  }

  public static void waitUntil(
      String description,
      UsableSupplier<Boolean> condition,
      long sleepIncrements,
      long timeout, TimeUnit timeoutUnit,
      boolean allowExceptions
  ) throws InterruptedException {
    long start = System.currentTimeMillis();
    long deadline = start + timeoutUnit.toMillis(timeout);
    long now = start;
    while (now < deadline) {
      try {
        if (condition.get()) {
          return;
        }
      } catch (Exception e) {
        if (!allowExceptions) {
          throw new RuntimeException(e);
        }
      }
      Thread.sleep(sleepIncrements);
      now = System.currentTimeMillis();
    }
    throw new IllegalStateException("condition " + description + " did not turn true within " + timeout + " " + timeoutUnit);
  }

  public static String getRandomString(int length) {
    char[] chars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    Random random = new Random();
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      stringBuilder.append(chars[Math.abs(random.nextInt()) % 16]);
    }
    return stringBuilder.toString();
  }

  public static String getExceptionString(int length) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(EXCEPTION_MESSAGE);
    for (int i = EXCEPTION_MESSAGE.length(); i < length; i++) {
      stringBuilder.append('X');
    }

    return stringBuilder.toString();
  }

  @FunctionalInterface
  public interface Task {
    void run() throws Exception;
  }

  @FunctionalInterface
  public interface UsableSupplier<T> {
    T get() throws Exception;
  }
}
