package io.confluent.csid.asyncconsumer.examples.streams;

import io.confluent.csid.asyncconsumer.integrationTests.KafkaTest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

@Slf4j
public class StreamsAppTest extends KafkaTest<String, String> {

    TopicPartition tp = new TopicPartition(StreamsApp.inputTopic, 0);

    @SneakyThrows
    @Test
    public void test() {
        log.info("Test start");
        ensureTopic(StreamsApp.inputTopic, 1);
        ensureTopic(StreamsApp.outputTopicName, 1);

        StreamsAppUnderTest coreApp = new StreamsAppUnderTest();

        coreApp.run();

        Producer<String, String> kafkaProducer = kcu.createNewProducer(false);
        kafkaProducer.send(new ProducerRecord<>(StreamsApp.inputTopic, "a key 1", "a value"));
        kafkaProducer.send(new ProducerRecord<>(StreamsApp.inputTopic,"a key 2", "a value"));
        kafkaProducer.send(new ProducerRecord<>(StreamsApp.inputTopic,"a key 3", "a value"));

        Awaitility.await().untilAsserted(()->{
            Assertions.assertThat(coreApp.messageCount.get()).isEqualTo(3);
        });

        coreApp.close();
    }

    class StreamsAppUnderTest extends StreamsApp {

        @Override
        Consumer<String, String> getKafkaConsumer() {
            return kcu.consumer;
        }

        @Override
        Producer<String, String> getKafkaProducer() {
            return kcu.createNewProducer(true);
        }

        @Override
        String getServerConfig() {
            return KafkaTest.kafkaContainer.getBootstrapServers();
        }
    }
}
