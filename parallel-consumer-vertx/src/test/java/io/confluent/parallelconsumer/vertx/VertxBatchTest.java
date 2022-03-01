package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.parallelconsumer.BatchTestBase;
import io.confluent.parallelconsumer.BatchTestMethods;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.RateLimiter;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
@ExtendWith(VertxExtension.class)
public class VertxBatchTest extends VertxBaseUnitTest implements BatchTestBase {

    private Vertx vertx;
    private VertxTestContext tc;

    BatchTestMethods<Future<String>> batchTestMethods;

    @BeforeEach
    void setup() {
        batchTestMethods = new BatchTestMethods<>(this) {

            @Override
            protected KafkaTestUtils getKtu() {
                return ktu;
            }

            @SneakyThrows
            @Override
            protected Future<String> averageBatchSizeTestPollStep(PollContext<String, String> recordList) {
                int delayInMs = 30;

                Promise<String> promise = Promise.promise();

                vertx.setTimer(delayInMs, event -> {
                    String msg = msg("Saw batch or records: {}", recordList.getOffsetsFlattened());
                    log.debug(msg);
                    promise.complete(msg);
                });

                return promise.future();
            }

            @Override
            protected void averageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger) {
                vertxAsync.batchVertxFuture(recordList -> {
                    return averageBatchSizeTestPollInner(numBatches, numRecords, statusLogger, recordList);
                });
            }

            @Override
            protected AbstractParallelEoSStreamProcessor getPC() {
                return vertxAsync;
            }

            @Override
            public void simpleBatchTestPoll(List<PollContext<String, String>> batchesReceived) {
                vertxAsync.batchVertxFuture(recordList -> {
//                    return vertx.executeBlocking(event -> {
                    String msg = msg("Saw batch or records: {}", recordList.getOffsetsFlattened());
                    log.debug(msg);
                    batchesReceived.add(recordList);
//                    return Future.succeededFuture(msg);
                    return Future.failedFuture(new RuntimeException("testing failure"));
                    //event.complete(msg("Saw batch or records: {}", recordList.getOffsetsFlattened()));
//
//                    });
                });
            }

            @Override
            protected void batchFailPoll(List<PollContext<String, String>> receivedBatches) {
                vertxAsync.batchVertxFuture(pollBatch -> {
                    receivedBatches.add(pollBatch);
                    batchFailPollInner(pollBatch);
                    return Future.succeededFuture(msg("Saw batch or records: {}", pollBatch.getOffsetsFlattened()));
                });
            }
        };
    }

    @Test
    void averageBatchSizeTest(Vertx vertx, VertxTestContext tc) {
        this.vertx = vertx;
        this.tc = tc;
        averageBatchSizeTest();
        tc.completeNow();
    }

    @Override
    public void averageBatchSizeTest() {
        batchTestMethods.averageBatchSizeTest(100_000);
    }

    @ParameterizedTest
    @EnumSource
    void simpleBatchTest(ParallelConsumerOptions.ProcessingOrder order, Vertx vertx, VertxTestContext tc) {
        this.vertx = vertx;
        this.tc = tc;
        simpleBatchTest(order);
        tc.completeNow();
    }

    @Override
    public void simpleBatchTest(ParallelConsumerOptions.ProcessingOrder order) {
        batchTestMethods.simpleBatchTest(order);
    }


    @ParameterizedTest
    @EnumSource
    public void batchFailureTest(ParallelConsumerOptions.ProcessingOrder order, Vertx vertx, VertxTestContext tc) {
        this.vertx = vertx;
        this.tc = tc;
        batchFailureTest(order);
        tc.completeNow();
    }

    @Override
    public void batchFailureTest(ParallelConsumerOptions.ProcessingOrder order) {
        batchTestMethods.batchFailureTest(order);
    }
}
