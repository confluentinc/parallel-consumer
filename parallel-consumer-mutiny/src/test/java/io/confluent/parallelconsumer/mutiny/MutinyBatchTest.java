package io.confluent.parallelconsumer.mutiny;

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.parallelconsumer.BatchTestBase;
import io.confluent.parallelconsumer.BatchTestMethods;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.RateLimiter;
import io.smallrye.mutiny.Uni;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
public class MutinyBatchTest extends MutinyUnitTestBase implements BatchTestBase {

    BatchTestMethods<Uni<String>> batchTestMethods;

    @BeforeEach
    void setup() {
        batchTestMethods = new BatchTestMethods<>(this) {

            @Override
            protected KafkaTestUtils getKtu() {
                return ktu;
            }

            @SneakyThrows
            @Override
            protected Uni<String> averageBatchSizeTestPollStep(PollContext<String, String> recordList) {
                return Uni.createFrom()
                        .item(msg("Saw batch or records: {}", recordList.getOffsetsFlattened()))
                        .onItem().delayIt().by(Duration.ofMillis(30));
            }

            @Override
            protected void averageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger) {
                mutinyPC.onRecord(recordList ->
                        averageBatchSizeTestPollInner(numBatches, numRecords, statusLogger, recordList)
                );
            }

            @Override
            protected AbstractParallelEoSStreamProcessor getPC() {
                return mutinyPC;
            }

            @Override
            public void simpleBatchTestPoll(List<PollContext<String, String>> batchesReceived) {
                mutinyPC.onRecord(recordList -> {
                    String msg = msg("Saw batch or records: {}", recordList.getOffsetsFlattened());
                    log.debug(msg);
                    batchesReceived.add(recordList);
                    return Uni.createFrom().item(msg);
                });
            }

            @Override
            protected void batchFailPoll(List<PollContext<String, String>> batchesReceived) {
                mutinyPC.onRecord(recordList -> {
                    batchFailPollInner(recordList);
                    batchesReceived.add(recordList);
                    return Uni.createFrom().item(msg("Saw batch or records: {}", recordList.getOffsetsFlattened()));
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

