package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.parallelconsumer.controller.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.RateLimiter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic tests for batch processing functionality
 */
@Slf4j
public class CoreBatchTest extends ParallelEoSStreamProcessorTestBase implements BatchTestBase {

    BatchTestMethods<Void> batchTestMethods;

    @BeforeEach
    void setup() {
        batchTestMethods = new BatchTestMethods<>(this) {

            @Override
            protected KafkaTestUtils getKtu() {
                return ktu;
            }

            @SneakyThrows
            @Override
            protected Void averageBatchSizeTestPollStep(PollContext<String, String> recordList) {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                return null;
            }

            @Override
            protected void averageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter
                    statusLogger) {
                parallelConsumer.poll(pollBatch -> {
                    averageBatchSizeTestPollInner(numBatches, numRecords, statusLogger, pollBatch);
                });
            }

            @Override
            protected AbstractParallelEoSStreamProcessor getPC() {
                return parallelConsumer;
            }

            @Override
            public void simpleBatchTestPoll(List<PollContext<String, String>> batchesReceived) {
                parallelConsumer.poll(context -> {
                    log.debug("Batch of messages: {}", context.getOffsetsFlattened());
                    batchesReceived.add(context);
                });
            }

            @Override
            protected void batchFailPoll(List<PollContext<String, String>> receivedBatches) {
                parallelConsumer.poll(pollBatch -> {
                    batchFailPollInner(pollBatch);
                    receivedBatches.add(pollBatch);
                });
            }
        };
    }

    @Test
    public void averageBatchSizeTest() {
        batchTestMethods.averageBatchSizeTest(50000);
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
