package io.confluent.parallelconsumer.examples.streams;
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

@Slf4j
public class StreamsAppTest extends BrokerIntegrationTest {

    @SneakyThrows
    @Test
    public void test() {
        log.info("Test start");
        ensureTopic(StreamsApp.inputTopic, 1);
        ensureTopic(StreamsApp.outputTopicName, 1);

        StreamsAppUnderTest coreApp = new StreamsAppUnderTest();

        coreApp.run();

        try (Producer<String, String> kafkaProducer = getKcu().createNewProducer(false)) {

            kafkaProducer.send(new ProducerRecord<>(StreamsApp.inputTopic, "a key 1", "a value"));
            kafkaProducer.send(new ProducerRecord<>(StreamsApp.inputTopic, "a key 2", "a value"));
            kafkaProducer.send(new ProducerRecord<>(StreamsApp.inputTopic, "a key 3", "a value"));

            Awaitility.await().untilAsserted(() -> {
                Assertions.assertThat(coreApp.messageCount.get()).isEqualTo(3);
            });

        } finally {
            coreApp.close();
        }
    }

    class StreamsAppUnderTest extends StreamsApp {

        @Override
        Consumer<String, String> getKafkaConsumer() {
            return getKcu().getConsumer();
        }

        @Override
        Producer<String, String> getKafkaProducer() {
            return getKcu().createNewProducer(false);
        }

        @Override
        String getServerConfig() {
            return BrokerIntegrationTest.kafkaContainer.getBootstrapServers();
        }
    }
}
