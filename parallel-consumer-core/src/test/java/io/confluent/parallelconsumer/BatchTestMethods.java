package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.RateLimiter;
import io.confluent.parallelconsumer.truth.LongPollingMockConsumerSubject;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Batch test which can be used in the different modules. The need for this is because the batch methods in each module
 * all have different signatures, and return types.
 */
@Slf4j
@RequiredArgsConstructor
public abstract class BatchTestMethods<POLL_RETURN> {

    public static final long FAILURE_TARGET = 5L;
    private final ParallelEoSStreamProcessorTestBase baseTest;

    protected abstract KafkaTestUtils getKtu();


    protected void setupParallelConsumer(int targetBatchSize, int maxConcurrency, ParallelConsumerOptions.ProcessingOrder ordering) {
        //
        ParallelConsumerOptions<Object, Object> options = ParallelConsumerOptions.builder()
                .batchSize(targetBatchSize)
                .ordering(ordering)
                .maxConcurrency(maxConcurrency)
                .build();
        baseTest.setupParallelConsumerInstance(options);

        //
        baseTest.parentParallelConsumer.setTimeBetweenCommits(ofSeconds(5));
    }

    protected abstract AbstractParallelEoSStreamProcessor getPC();

    public void averageBatchSizeTest(int numRecsExpected) {
        final int targetBatchSize = 20;
        int maxConcurrency = 8;

        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, numRecsExpected);

        setupParallelConsumer(targetBatchSize, maxConcurrency, UNORDERED);

        //
        getKtu().sendRecords(numRecsExpected);

        //
        var numBatches = new AtomicInteger(0);
        var numRecordsProcessed = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        RateLimiter statusLogger = new RateLimiter(1);

        averageBatchSizeTestPoll(numBatches, numRecordsProcessed, statusLogger);

        //
        waitAtMost(defaultTimeout).alias("expected number of records")
                .failFast(() -> getPC().isClosedOrFailed())
                .untilAsserted(() -> {
                    bar.stepTo(numRecordsProcessed.get());
                    assertThat(numRecordsProcessed.get()).isEqualTo(numRecsExpected);
                });
        bar.close();

        //

        //
        double targetMetThreshold = 999. / 1000.;
        double acceptableAttainedBatchSize = targetBatchSize * targetMetThreshold;
        double averageBatchSize = calcAverage(numRecordsProcessed, numBatches);
        assertThat(averageBatchSize).isGreaterThan(acceptableAttainedBatchSize);

        baseTest.parentParallelConsumer.requestCommitAsap();
        baseTest.awaitForCommit(numRecsExpected);
        var duration = System.currentTimeMillis() - start;
        log.info("Processed {} records in {} ms. Average batch size was: {}. {} records per second.", numRecsExpected, duration, averageBatchSize, numRecsExpected / (duration / 1000.0));

        assertThat(getPC().isClosedOrFailed()).isFalse();

        LongPollingMockConsumerSubject.assertThat(getKtu().getConsumerSpy())
                .hasCommittedToAnyPartition()
                .atLeastOffset(numRecsExpected);
    }

    /**
     * Must call {@link #averageBatchSizeTestPollInner}
     */
    protected abstract void averageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger);

    protected POLL_RETURN averageBatchSizeTestPollInner(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger, PollContext<String, String> pollBatch) {
        int size = (int) pollBatch.size();

        statusLogger.performIfNotLimited(() -> {
            try {
                log.debug(
                        "Processed {} records in {} batches with average size {}",
                        numRecords.get(),
                        numBatches.get(),
                        calcAverage(numRecords, numBatches)
                );
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });

        try {
            log.trace("Batch size {}", size);
            return averageBatchSizeTestPollStep(pollBatch);
        } finally {
            numBatches.getAndIncrement();
            numRecords.addAndGet(size);
        }
    }

    protected abstract POLL_RETURN averageBatchSizeTestPollStep(PollContext<String, String> recordList);

    private double calcAverage(AtomicInteger numRecords, AtomicInteger numBatches) {
        return numRecords.get() / (0.0 + numBatches.get());
    }


    @SneakyThrows
    public void simpleBatchTest(ParallelConsumerOptions.ProcessingOrder order) {
        int batchSizeSetting = 2;
        int numRecsExpected = 5;

        getPC().setTimeBetweenCommits(ofSeconds(1));

        setupParallelConsumer(batchSizeSetting, ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY, order);

        var recs = getKtu().sendRecords(numRecsExpected);
        List<PollContext<String, String>> batchesReceived = new CopyOnWriteArrayList<>();

        //
        simpleBatchTestPoll(batchesReceived);

        //
        int expectedNumOfBatches = (order == PARTITION) ?
                numRecsExpected : // partition ordering restricts the batch sizes to a single element as all records are in a single partition
                (int) Math.ceil(numRecsExpected / (double) batchSizeSetting);

        waitAtMost(defaultTimeout).alias("expected number of batches")
                .failFast(() -> getPC().isClosedOrFailed())
                .untilAsserted(() -> {
                    assertThat(batchesReceived).hasSize(expectedNumOfBatches);
                    assertThat(batchesReceived.stream().mapToLong(Collection::size).sum()).isEqualTo(numRecsExpected);
                });

        assertThat(batchesReceived)
                .as("batch size")
                .allSatisfy(receivedBatchEntry -> assertThat(receivedBatchEntry).hasSizeLessThanOrEqualTo(batchSizeSetting))
                .as("all messages processed")
                .flatExtracting(PollContext::getConsumerRecordsFlattened).hasSameElementsAs(recs);

        assertThat(getPC().isClosedOrFailed()).isFalse();

        baseTest.awaitForCommit(numRecsExpected);
        getPC().closeDrainFirst();
    }

    public abstract void simpleBatchTestPoll(List<PollContext<String, String>> batchesReceived);

    @SneakyThrows
    public void batchFailureTest(ParallelConsumerOptions.ProcessingOrder order) {
        int batchSize = 5;
        int expectedNumOfMessages = 20;

        setupParallelConsumer(batchSize, ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY, order);

        var recs = getKtu().sendRecords(expectedNumOfMessages);
        List<PollContext<String, String>> receivedBatches = Collections.synchronizedList(new ArrayList<>());

        //
        batchFailPoll(receivedBatches);

        //
        int expectedNumOfBatches = (int) Math.ceil(expectedNumOfMessages / (double) batchSize);

        //
        baseTest.awaitForCommit(expectedNumOfMessages);

        // due to the failure, might get one extra batch
        assertThat(receivedBatches).hasSizeGreaterThanOrEqualTo(expectedNumOfBatches);

        assertThat(receivedBatches)
                .as("batch size")
                .allSatisfy(receivedBatch ->
                        assertThat(receivedBatch).hasSizeLessThanOrEqualTo(batchSize))
                .as("all messages processed")
                .flatExtracting(PollContext::getConsumerRecordsFlattened).hasSameElementsAs(recs);

        //
        assertThat(getPC().isClosedOrFailed()).isFalse();
    }

    /**
     * Must call {@link #batchFailPollInner}
     */
    protected abstract void batchFailPoll(List<PollContext<String, String>> receivedBatches);

    protected POLL_RETURN batchFailPollInner(PollContext<String, String> pollBatch) {
        List<Long> offsets = pollBatch.getOffsetsFlattened();
        log.debug("Got batch {}", offsets);

        boolean contains = offsets.contains(FAILURE_TARGET);
        if (contains) {
            var target = pollBatch.stream().filter(x -> x.offset() == FAILURE_TARGET).findFirst().get();
            int numberOfFailedAttempts = target.getNumberOfFailedAttempts();
            int targetAttempts = 3;
            if (numberOfFailedAttempts < targetAttempts) {
                log.debug("Failing batch containing target offset {}", FAILURE_TARGET);
                throw new FakeRuntimeError(msg("Testing failure processing a batch - pretend attempt #{}", numberOfFailedAttempts));
            } else {
                log.debug("Failing target {} now completing as has has reached target attempts {}", offsets, targetAttempts);
            }
        }
        log.debug("Completing batch {}", offsets);
        return null;
    }

}
