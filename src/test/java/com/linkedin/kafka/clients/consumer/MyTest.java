package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.utils.tests.AbstractKafkaClientsIntegrationTestHarness;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class MyTest extends AbstractKafkaClientsIntegrationTestHarness {

    @BeforeTest
    @Override
    public void setUp() {
        super.setUp();
    }

    @AfterTest
    @Override
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void test() {
        LiKafkaConsumer<String, String> consumer = createConsumer(null);
        consumer.listTopics();
    }
}
