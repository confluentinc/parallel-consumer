package io.confluent.parallelconsumer.examples.reactor;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import io.confluent.csid.utils.WireMockUtils;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.HashMap;

import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
class ReactorAppTest {

    TopicPartition tp = new TopicPartition(ReactorApp.inputTopic, 0);

    @Timeout(20)
    @SneakyThrows
    @Test
    void test() {
        log.info("Test start");
        WireMockServer wireMockServer = new WireMockUtils().setupWireMock();
        int port = wireMockServer.port();

        ReactorAppAppUnderTest coreApp = new ReactorAppAppUnderTest(port);

        coreApp.run();

        coreApp.mockConsumer.addRecord(new ConsumerRecord(ReactorApp.inputTopic, 0, 0, "a key 1", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(ReactorApp.inputTopic, 0, 1, "a key 2", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(ReactorApp.inputTopic, 0, 2, "a key 3", "a value"));

        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            KafkaTestUtils.assertLastCommitIs(coreApp.mockConsumer, 3);
        });

        coreApp.close();
    }

    @RequiredArgsConstructor
    class ReactorAppAppUnderTest extends ReactorApp {

        private final int port;

        LongPollingMockConsumer<String, String> mockConsumer = Mockito.spy(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));

        @Override
        Consumer<String, String> getKafkaConsumer() {
            HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(tp, 0L);
            mockConsumer.updateBeginningOffsets(beginningOffsets);
            Mockito.when(mockConsumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer
            return mockConsumer;
        }

        @Override
        Producer<String, String> getKafkaProducer() {
            return new MockProducer<>(true, null, null);
        }

        @Override
        public void postSetup() {
            mockConsumer.subscribeWithRebalanceAndAssignment(of(inputTopic), 1);
        }

        @Override
        protected int getPort() {
            return port;
        }
    }
}
