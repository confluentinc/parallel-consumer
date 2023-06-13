
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
class ReactorBatchAppTest {

    TopicPartition tp = new TopicPartition(ReactorBatchApp.inputTopic, 0);

    @Timeout(20)
    @SneakyThrows
    @Test
    void testBatchBetweenMinAndHigh() {
        log.info("Test start");
        WireMockServer wireMockServer = new WireMockUtils().setupWireMock();
        int port = wireMockServer.port();

        ReactorBatchAppAppUnderTest coreApp = new ReactorBatchAppAppUnderTest(port);

        coreApp.run();
        var offset = 0;
        for(int i = 0; i < ReactorBatchApp.batchSize * 2; i++){
            coreApp.mockConsumer.addRecord(new ConsumerRecord(ReactorBatchApp.inputTopic, 0, offset++, "a key " + i, "a value"));
        }

        coreApp.mockConsumer.addRecord(new ConsumerRecord(ReactorBatchApp.inputTopic, 0, offset++, "last batch", "a value"));

        final int lastOffset = offset;
        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            KafkaTestUtils.assertLastCommitIs(coreApp.mockConsumer, lastOffset);
        });

        final int firstBatchSize = coreApp.batchSizes.get(0);
        assertThat(coreApp.batchSizes.size()).isEqualTo(3);
        assertThat(coreApp.batchSizes.get(0)).isLessThanOrEqualTo(ReactorBatchApp.batchSize);
        assertThat(coreApp.batchSizes.get(0)).isGreaterThanOrEqualTo(ReactorBatchApp.minBatchSize);
        assertThat(coreApp.batchSizes.get(1)).isLessThanOrEqualTo(ReactorBatchApp.batchSize);
        assertThat(coreApp.batchSizes.get(1)).isGreaterThanOrEqualTo(ReactorBatchApp.minBatchSize);
        //should happen due to timeout
        int allBatchesSize = coreApp.batchSizes.stream().mapToInt(Integer::intValue).sum();
        assertThat(allBatchesSize).isEqualTo(ReactorBatchApp.batchSize * 2 + 1);
        coreApp.close();
    }

    @Timeout(20)
    @SneakyThrows
    @Test
    void testSlowTrafficHighTimeout() {
        log.info("Test start");
        WireMockServer wireMockServer = new WireMockUtils().setupWireMock();
        int port = wireMockServer.port();
        int maxTimeout = 3 * 1000;
        ReactorBatchAppAppUnderTest coreApp = new ReactorBatchAppAppUnderTest(port,maxTimeout );

        coreApp.run();
        var offset = 0;
        int allBatchSize = ReactorBatchApp.minBatchSize * 5;
        for(int i = 0; i < ReactorBatchApp.minBatchSize * 5; i++){
            coreApp.mockConsumer.addRecord(new ConsumerRecord(ReactorBatchApp.inputTopic, 0, offset++, UUID.randomUUID().toString(), "a value"));
            Thread.sleep(10);
        }

        final int lastOffset = offset;
        Awaitility.await().pollInterval(Duration.ofMillis(maxTimeout)).untilAsserted(() -> {
            KafkaTestUtils.assertLastCommitIs(coreApp.mockConsumer, lastOffset);
        });

        for(int i = 0 ; i < coreApp.batchSizes.size() - 1; i++){// all batches except the last one!
            assertThat(coreApp.batchSizes.get(i)).isLessThanOrEqualTo(ReactorBatchApp.batchSize);
            assertThat(coreApp.batchSizes.get(i)).isGreaterThanOrEqualTo(ReactorBatchApp.minBatchSize);
        }
        int allBatchesSize = coreApp.batchSizes.stream().mapToInt(Integer::intValue).sum();
        assertThat(allBatchesSize).isEqualTo(allBatchSize);
        coreApp.close();
    }

    @RequiredArgsConstructor
    class ReactorBatchAppAppUnderTest extends ReactorBatchApp {

        private final int port;

        public ReactorBatchAppAppUnderTest(int port, int timeout){
            this.port = port;
            this.minBatchTimeout = timeout;
        }

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

    }
}
