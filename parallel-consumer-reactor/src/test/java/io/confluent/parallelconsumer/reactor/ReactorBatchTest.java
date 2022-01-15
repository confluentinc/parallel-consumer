package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.parallelconsumer.BatchTestBase;
import io.confluent.parallelconsumer.BatchTestMethods;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.RateLimiter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
public class ReactorBatchTest extends ReactorUnitTestBase implements BatchTestBase {

    BatchTestMethods<Mono<String>> batchTestMethods;

    @BeforeEach
    void setup() {
        batchTestMethods = new BatchTestMethods<>(this) {

            @Override
            protected KafkaTestUtils getKtu() {
                return ktu;
            }

            @SneakyThrows
            @Override
            protected Mono<String> averageBatchSizeTestPollStep(List<ConsumerRecord<String, String>> recordList) {
                return Mono.just(msg("Saw batch or records: {}", toOffsets(recordList)))
                        .delayElement(Duration.ofMillis(30));
            }

            @Override
            protected void averageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger) {
                reactorPC.reactBatch(recordList -> {
                    return averageBatchSizeTestPollInner(numBatches, numRecords, statusLogger, recordList);
                });
            }

            @Override
            protected AbstractParallelEoSStreamProcessor getPC() {
                return reactorPC;
            }

            @Override
            public void simpleBatchTestPoll(List<List<ConsumerRecord<String, String>>> batchesReceived) {
                reactorPC.reactBatch(recordList -> {
                    log.debug("Batch of messages: {}", toOffsets(recordList));
                    batchesReceived.add(recordList);
                    return Mono.just(msg("Saw batch or records: {}", toOffsets(recordList)));
                });
            }

            @Override
            protected void batchFailPoll(List<List<ConsumerRecord<String, String>>> batchesReceived) {
                reactorPC.reactBatch(recordList -> {
                    batchFailPollInner(recordList);
                    batchesReceived.add(recordList);
                    return Mono.just(msg("Saw batch or records: {}", toOffsets(recordList)));
                });
            }
        };
    }

    @Test
    public void averageBatchSizeTest() {
        batchTestMethods.averageBatchSizeTest(10000);
    }

    @ParameterizedTest
    @EnumSource
    @Override
    public void simpleBatchTest(ParallelConsumerOptions.ProcessingOrder order) {
        batchTestMethods.simpleBatchTest(order);
    }

    @ParameterizedTest
    @EnumSource
    @Override
    public void batchFailureTest(ParallelConsumerOptions.ProcessingOrder order) {
        batchTestMethods.batchFailureTest(order);
    }

}
