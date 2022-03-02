package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;

/**
 * Test for pause/resume feature of the parallel consumer (see {@code GH#193}).
 *
 * @author niels.oertel
 */
@Slf4j
class ParallelEoSStreamProcessorPauseResumeTest extends ParallelEoSStreamProcessorTestBase {

    private static final AtomicLong MY_ID_GENERATOR = new AtomicLong();

    private static final AtomicLong RECORD_BATCH_KEY_GENERATOR = new AtomicLong();

    private static class TestUserFunction implements Consumer<ConsumerRecord<String, String>> {

        private final AtomicLong numProcessedRecords = new AtomicLong();

        /**
         * The number of in flight records. Note that this may not exactly match the real number of in flight records as
         * parallel consumer has a wrapper around the user function so incrementing/decrementing the counter is a little
         * bit delayed.
         */
        private final AtomicInteger numInFlightRecords = new AtomicInteger();

        private final ReentrantLock mutex = new ReentrantLock();

        public void lockProcessing() {
            mutex.lock();
        }

        public void unlockProcessing() {
            mutex.unlock();
        }

        @Override
        public void accept(ConsumerRecord<String, String> t) {
            log.debug("Received: {}", t);
            numInFlightRecords.incrementAndGet();
            try {
                lockProcessing();
                numProcessedRecords.incrementAndGet();
            } finally {
                unlockProcessing();
                numInFlightRecords.decrementAndGet();
            }
        }

        public void reset() {
            numProcessedRecords.set(0L);
        }
    }

    private ParallelConsumerOptions<String, String> getBaseOptions(final CommitMode commitMode, int maxConcurrency) {
        return ParallelConsumerOptions.<String, String>builder()
                .commitMode(commitMode)
                .consumer(consumerSpy)
                .maxConcurrency(maxConcurrency)
                .build();
    }

    private void addRecords(final int numRecords) {
        long recordBatchKey = RECORD_BATCH_KEY_GENERATOR.incrementAndGet();
        log.debug("Producing {} records with batch key {}.", numRecords, recordBatchKey);
        for (int i = 0; i < numRecords; ++i) {
            consumerSpy.addRecord(ktu.makeRecord("key-" + recordBatchKey + i, "v0-test-" + i));
        }
        log.debug("Finished producing {} records with batch key {}.", numRecords, recordBatchKey);
    }

    private void setupParallelConsumerInstanceAndLogCapture(final CommitMode commitMode, final int maxConcurrency) {
        setupParallelConsumerInstance(getBaseOptions(commitMode, maxConcurrency));

        // register unique ID on the parallel consumer
        String myId = "p/r-test-" + MY_ID_GENERATOR.incrementAndGet();
        parallelConsumer.setMyId(Optional.of(myId));
    }

    // todo delete
    private long getOverallCommittedOffset() {
        return getCommittedOffsetsByPartitions().values().stream().collect(Collectors.summingLong(Long::longValue));
    }

    // todo delete
    private Map<TopicPartition, Long> getCommittedOffsetsByPartitions() {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistory = getCommitHistory();
        if (commitHistory.isEmpty()) {
            return Collections.emptyMap();
        }

        Set<String> consumerGroups = commitHistory.stream().flatMap(c -> c.keySet().stream())
                .collect(Collectors.toSet());
        // verify that test setup is correct (this method only supports a single consumer group for now)
        assertThat(consumerGroups).hasSize(1);
        String consumerGroupName = consumerGroups.iterator().next();

        // get the last committed offse for each partitions
        Map<TopicPartition, Long> result = new HashMap<>();
        for (Map<String, Map<TopicPartition, OffsetAndMetadata>> commit : getCommitHistory()) {
            commit.getOrDefault(consumerGroupName, Collections.emptyMap())
                    .forEach((partition, offsetAndMetadata) -> result.put(partition, offsetAndMetadata.offset()));
        }
        return result;
    }

    private TestUserFunction createTestSetup(final CommitMode commitMode, final int maxConcurrency) {
        // setup parallel consumer with custom processing function
        setupParallelConsumerInstanceAndLogCapture(commitMode, maxConcurrency);
        TestUserFunction testUserFunction = new TestUserFunction();
        parallelConsumer.poll(testUserFunction);

        // ensure that commit offset start at 0 -> otherwise there is a bug in the test setup
        awaitForCommit(0);

        return testUserFunction;
    }

    /**
     * This test verifies that no new records are submitted to the workers once the consumer is paused.
     *
     * @param commitMode The commit mode to be configured for the parallel consumer.
     */
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void pausingAndResumingProcessingShouldWork(final CommitMode commitMode) {
        int numTestRecordsPerBatch = 1_000;
        long totalRecordsExpected = 2L * numTestRecordsPerBatch;

        TestUserFunction testUserFunction = createTestSetup(commitMode, 3);

        // produce some messages
        addRecords(numTestRecordsPerBatch);
//        ktu.sendRecords(numTestRecordsPerBatch);

        // wait for processing to finish
        Awaitility
                .waitAtMost(defaultTimeout)
                .pollDelay(50L, TimeUnit.MILLISECONDS)
                .alias(numTestRecordsPerBatch + " records should be processed")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(numTestRecordsPerBatch));
//                .until(testUserFunction.numProcessedRecords::get, numRecords -> numRecords == numTestRecordsPerBatch);
        // overall committed offset should reach the same value
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias("sum of consumer offsets should reach " + numTestRecordsPerBatch)
                .untilAsserted(() -> assertThat(getOverallCommittedOffset()).isEqualTo(numTestRecordsPerBatch));
        //                .until(this::getOverallCommittedOffset, numRecords -> numTestRecordsPerBatch == numRecords);
        testUserFunction.reset();

        // pause parallel consumer and wait for control loops to catch up
        parallelConsumer.pauseIfRunning();
//
//        controlLoopTracker.waitForSomeParallelStreamProcessorControlLoopCycles(1, defaultTimeout);
//        controlLoopTracker.waitForSomeBrokerPollSystemControlLoopCycles(1, defaultTimeout);

        awaitForOneLoopCycle();

        // produce more messages -> nothing should be processed
        addRecords(numTestRecordsPerBatch);
//        controlLoopTracker.waitForSomeControlLoopCycles(5, defaultTimeout);
        awaitForSomeLoopCycles(2);

        // shouldn't have produced any records
        assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(0L);

        // overall committed offset should stay at old value
        assertThat(getOverallCommittedOffset()).isEqualTo(numTestRecordsPerBatch);

        // resume parallel consumer ->
        parallelConsumer.resumeIfPaused();

        // messages should be processed now
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(numTestRecordsPerBatch + " records should be processed")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(numTestRecordsPerBatch));
//            .until(testUserFunction.numProcessedRecords::get, numRecords -> numTestRecordsPerBatch == numRecords);
        // overall committed offset should reach the total of two batches that were processed
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias("sum of consumer offsets should reach " + totalRecordsExpected)
//            .until(this::getOverallCommittedOffset, numRecords -> totalRecordsExpected == numRecords);
                .untilAsserted(() -> assertThat(getOverallCommittedOffset()).isEqualTo(totalRecordsExpected));
    }

    /**
     * This test verifies that in flight work is finished successfully when the consumer is paused. In flight work is
     * work that's currently being processed inside a user function has already been submitted to be processed based on
     * the dynamic load factor. The test also verifies that new offsets are committed once the in-flight work finishes
     * even if the consumer is still paused.
     *
     * @param commitMode The commit mode to be configured for the parallel consumer.
     */
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void testThatInFlightWorkIsFinishedSuccessfullyAndOffsetsAreCommitted(final CommitMode commitMode) {
        int degreeOfParallelism = 3;
        int numTestRecordsPerBatch = 1_000;

        TestUserFunction testUserFunction = createTestSetup(commitMode, degreeOfParallelism);
        // block processing in the user function to ensure we have in flight work once we pause the consumer
        testUserFunction.lockProcessing();

        // produce some messages
        addRecords(numTestRecordsPerBatch);

        // wait until we have enough records in flight
        Awaitility
                .waitAtMost(defaultTimeout)
                .pollDelay(50L, TimeUnit.MILLISECONDS)
                .alias(degreeOfParallelism + " records should be in flight processed")
                .until(testUserFunction.numInFlightRecords::get, numInFlightRecords -> degreeOfParallelism == numInFlightRecords);

        // overall committed consumer offset should still be at 0
        assertThat(getOverallCommittedOffset()).isEqualTo(0L);

        // pause parallel consumer and wait for control loops to catch up
        parallelConsumer.pauseIfRunning();
        awaitForOneLoopCycle();
//        controlLoopTracker.waitForSomeParallelStreamProcessorControlLoopCycles(1, defaultTimeout);
//        controlLoopTracker.waitForSomeBrokerPollSystemControlLoopCycles(1, defaultTimeout);

        // unlock the user function
        testUserFunction.unlockProcessing();

        // in flight messages + buffered messages should get processed now (exact number is based on dynamic load factor)
        Awaitility
                .waitAtMost(defaultTimeout)
                .pollDelay(50L, TimeUnit.MILLISECONDS)
                .alias("at least " + degreeOfParallelism + " records should be processed")
                .until(testUserFunction.numProcessedRecords::get, numRecords -> degreeOfParallelism <= numRecords);

        // overall committed offset should reach the same value
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias("sum of consumer offsets should reach number of processed records")
                .until(this::getOverallCommittedOffset, numRecords -> testUserFunction.numProcessedRecords.get() == numRecords);

        // shouldn't have anymore in flight records now
        assertThat(testUserFunction.numInFlightRecords.get()).isEqualTo(0);
        assertThat(parallelConsumer.getWm().getNumberRecordsOutForProcessing()).isEqualTo(0);

        // resume parallel consumer ->
        parallelConsumer.resumeIfPaused();

        // other pending messages should be processed now
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(numTestRecordsPerBatch + " records should be processed")
                .until(testUserFunction.numProcessedRecords::get, numRecords -> numTestRecordsPerBatch == numRecords);
        // overall committed offset should reach the total number of processed records
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias("sum of consumer offsets should reach number of processed records")
                .until(this::getOverallCommittedOffset, numRecords -> testUserFunction.numProcessedRecords.get() == numRecords);
        testUserFunction.reset();
    }

}
