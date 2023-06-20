package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
class CoreAppBatchTest {

    @SneakyThrows
    @Test
    public void testOnlyGetMinBeforeTimeout() {
        log.info("Test start");
        CoreBatchAppUnderTest coreApp = new CoreBatchAppUnderTest(20 * 1000);

        coreApp.runPollAndProduce();

        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 0, "a key 1", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 1, "a key 2", "a value"));
        List<Map<TopicPartition, OffsetAndMetadata>> commits = coreApp.mockConsumer.getCommitHistoryInt();
        Assertions.assertThat(commits).isEmpty();

        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 2, "a key 3", "a value"));
        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            KafkaTestUtils.assertLastCommitIs(coreApp.mockConsumer, 3);
        });


        coreApp.close();
    }

    @SneakyThrows
    @Test
    public void testLowTraffic() {
        log.info("Test start");
        int maxTimeout = 3 * 1000;
        CoreBatchAppUnderTest coreApp = new CoreBatchAppUnderTest(maxTimeout);

        coreApp.runPollAndProduce();

        var offset = 0;
        int allBatchSize = CoreBatchAppUnderTest.minBatchSize * 5;
        for (int i = 0; i < CoreBatchAppUnderTest.minBatchSize * 5; i++) {
            coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, offset++, UUID.randomUUID().toString(), "a value"));
            Thread.sleep(10);
        }

        final int lastOffset = offset;
        Awaitility.await().pollInterval(Duration.ofMillis(maxTimeout)).untilAsserted(() -> {
            KafkaTestUtils.assertLastCommitIs(coreApp.mockConsumer, lastOffset);
        });

        for (int i = 0; i < coreApp.batchSizes.size() - 1; i++) {// all batches except the last one!
            assertThat(coreApp.batchSizes.get(i)).isLessThanOrEqualTo(CoreBatchWithMinApp.batchSize);
            assertThat(coreApp.batchSizes.get(i)).isGreaterThanOrEqualTo(CoreBatchWithMinApp.minBatchSize);
        }
        int allBatchesSize = coreApp.batchSizes.stream().mapToInt(Integer::intValue).sum();
        assertThat(allBatchesSize).isEqualTo(allBatchSize);
        coreApp.close();


        coreApp.close();
    }

    static class CoreBatchAppUnderTest extends CoreBatchWithMinApp {

        LongPollingMockConsumer<String, String> mockConsumer = Mockito.spy(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));
        TopicPartition tp = new TopicPartition(inputTopic, 0);

        public CoreBatchAppUnderTest(int timeout) {
            this.minBatchTimeout = timeout;
        }

        @Override
        Consumer<String, String> getKafkaConsumer() {
            when(mockConsumer.groupMetadata())
                    .thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer
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
