package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.consumer.HeaderKeySpace;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumerImpl;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * This is a simple code example we use in the documentation.
 */
public class DocumetationTest extends AbstractKafkaClientsIntegrationTestHarness {

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
  public void headerDocumentationTest() throws Exception {
    String topic = "header-example";
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapUrl());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    Producer<String, String> producer = new LiKafkaProducerImpl<>(producerProps);
    ExtensibleProducerRecord<String, String> xRecord =
        new ExtensibleProducerRecord<>(topic, 0, 0L, "key", "value");
    xRecord.header(HeaderKeySpace.PUBLIC_UNASSIGNED_START, new byte[] { 5, 6, 7, 7, 9});
    Future<RecordMetadata> future = producer.send(xRecord);
    future.get();

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapUrl());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testingConsumer");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    Consumer<String, String> consumer  = new LiKafkaConsumerImpl<>(consumerProps);
    consumer.assign(Collections.singletonList(new TopicPartition(topic, 0)));
    ConsumerRecords<String, String> consumerRecords = consumer.poll(5000);
    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
      ExtensibleConsumerRecord<String, String> consumerRecordWithHeaders =
          (ExtensibleConsumerRecord<String, String>) consumerRecord;
      byte[] headerValue = consumerRecordWithHeaders.header(HeaderKeySpace.PUBLIC_UNASSIGNED_START);
      //do something with headerValue
    }
  }
}
