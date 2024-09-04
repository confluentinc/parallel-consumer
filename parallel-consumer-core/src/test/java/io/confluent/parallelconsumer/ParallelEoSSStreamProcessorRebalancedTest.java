package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
class ParallelEoSSStreamProcessorRebalancedTest extends ParallelEoSStreamProcessorTestBase {

    private static final AtomicInteger RECORD_SET_KEY_GENERATOR = new AtomicInteger();

    @BeforeEach()
    public void setupAsyncConsumerTestBase() {
        setupTopicNames();
        setupClients();
    }

    @AfterEach()
    public void close() {
    }

    @ParameterizedTest
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void pausingAndResumingProcessingShouldWork(final CommitMode commitMode) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final PCModuleTestEnv pcModuleTestEnv = new PCModuleTestEnv(getBaseOptions(commitMode), countDownLatch);
        parallelConsumer = new ParallelEoSStreamProcessor<>(getBaseOptions(commitMode), pcModuleTestEnv);
        parentParallelConsumer = parallelConsumer;
        parallelConsumer.subscribe(of(INPUT_TOPIC));
        this.consumerSpy.subscribeWithRebalanceAndAssignment(of(INPUT_TOPIC), 2);
        attachLoopCounter(parallelConsumer);

        parallelConsumer.poll(context -> {
            //do nothing call never lands here
        });

        addRecordsWithSetKeyForEachPartition();
        awaitUntilTrue(() -> parallelConsumer.getWm().getNumberRecordsOutForProcessing() > 0);
        consumerSpy.revoke(of(new TopicPartition(INPUT_TOPIC, 0)));
        consumerSpy.rebalanceWithoutAssignment(consumerSpy.assignment());
        consumerSpy.assign(of(new TopicPartition(INPUT_TOPIC, 0)));

        addRecordsWithSetKeyForEachPartition();
        countDownLatch.countDown();
        awaitForCommit(4);
    }


    @ParameterizedTest
    @EnumSource(CommitMode.class)
    @SneakyThrows
    public void messageLossWithTimingPartitionRevoke() {
        // 1. insert one record
        // 2. before getWorkIfAvailable, the partition revoked
        // 3. the timing of revoking should be happening right after check isDirty
        // 4. the incompleteOffsets is cleared since it is overridden by RemovedPartitionState
        // 5. commit the offset, with empty incompleteOffsets
        // 6. container is marked as stale and be filtered
        CommitMode commitMode = CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final PCModuleTestEnv pcModuleTestEnv = new PCModuleTestEnv(getBaseOptions(commitMode), countDownLatch);
        parallelConsumer = new ParallelEoSStreamProcessor<>(getBaseOptions(commitMode), pcModuleTestEnv);
        parentParallelConsumer = parallelConsumer;
        parallelConsumer.subscribe(of(INPUT_TOPIC));
        this.consumerSpy.subscribeWithRebalanceAndAssignment(of(INPUT_TOPIC), 2);
        attachLoopCounter(parallelConsumer);
        primeFirstRecord();
        consumerSpy.revoke(of(new TopicPartition(INPUT_TOPIC, 0)));
        parallelConsumer.requestCommitAsap();
        parallelConsumer.poll(context -> {
            countDownLatch.countDown();
        });

        assertThat(countDownLatch.getCount()).isEqualTo(1L);
    }

    private void addRecordsWithSetKeyForEachPartition() {
        long recordSetKey = RECORD_SET_KEY_GENERATOR.incrementAndGet();
        log.debug("Producing {} records with set key {}.", 2, recordSetKey);
        consumerSpy.addRecord(ktu.makeRecord(0, "key-" + recordSetKey + 0, "v0-test-" + 0));
        consumerSpy.addRecord(ktu.makeRecord(1, "key-" + recordSetKey + 0, "v0-test-" + 0));
        log.debug("Finished producing {} records with set key {}.", 2, recordSetKey);
    }

    private ParallelConsumerOptions<String, String> getBaseOptions(final CommitMode commitMode) {
        final ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder =
                ParallelConsumerOptions.<String, String>builder()
                        .commitMode(commitMode)
                        .consumer(consumerSpy)
                        .batchSize(2)
                        .maxConcurrency(1);

        if (commitMode == CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER) {
            optionsBuilder.producer(producerSpy);
        }

        return optionsBuilder.build();
    }
}
