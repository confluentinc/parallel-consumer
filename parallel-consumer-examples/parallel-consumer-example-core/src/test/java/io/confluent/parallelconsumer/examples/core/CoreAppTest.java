package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.HashMap;

import static io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase.DEFAULT_GROUP_METADATA;
import static org.mockito.Mockito.when;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class CoreAppTest {

    @SneakyThrows
    @Test
    public void test() {
        log.info("Test start");
        CoreAppUnderTest coreApp = new CoreAppUnderTest();
        TopicPartition tp = new TopicPartition(coreApp.inputTopic, 0);

        coreApp.run();

        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 0, "a key 1", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 1, "a key 2", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 2, "a key 3", "a value"));

        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            KafkaTestUtils.assertLastCommitIs(coreApp.mockConsumer, 3);
        });

        coreApp.close();
    }

    @SneakyThrows
    @Test
    public void testPollAndProduce() {
        log.info("Test start");
        CoreAppUnderTest coreApp = new CoreAppUnderTest();

        coreApp.runPollAndProduce();

        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 0, "a key 1", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 1, "a key 2", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 2, "a key 3", "a value"));

        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            KafkaTestUtils.assertLastCommitIs(coreApp.mockConsumer, 3);
        });

        coreApp.close();
    }

    class CoreAppUnderTest extends CoreApp {

        LongPollingMockConsumer<String, String> mockConsumer = Mockito.spy(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));
        TopicPartition tp = new TopicPartition(inputTopic, 0);

        @Override
        Consumer<String, String> getKafkaConsumer() {
            when(mockConsumer.groupMetadata()).thenReturn(DEFAULT_GROUP_METADATA); // todo fix AK mock consumer
            return mockConsumer;
        }

        @Override
        Producer<String, String> getKafkaProducer() {
            var stringSerializer = Serdes.String().serializer();
            return new MockProducer<>(true, stringSerializer, stringSerializer);
        }

        @Override
        protected void postSetup() {
            mockConsumer.subscribeWithRebalanceAndAssignment(of(inputTopic), 1);
        }
    }
}
