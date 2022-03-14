package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static com.google.common.truth.Truth.assertThat;

/**
 * Test for pause/resume feature of the parallel consumer (see {@code GH#193}).
 *
 * @author niels.oertel
 */
@Slf4j
class ParallelEoSStreamProcessorPauseResumeTest extends ParallelEoSStreamProcessorTestBase {

    private static final AtomicInteger MY_ID_GENERATOR = new AtomicInteger();

    private static final AtomicInteger RECORD_SET_KEY_GENERATOR = new AtomicInteger();

    private static class TestUserFunction implements Consumer<ConsumerRecord<String, String>> {

        private final AtomicInteger numProcessedRecords = new AtomicInteger();

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
            numProcessedRecords.set(0);
        }
    }

    private ParallelConsumerOptions<String, String> getBaseOptions(final CommitMode commitMode, int maxConcurrency) {
        return ParallelConsumerOptions.<String, String>builder()
                .commitMode(commitMode)
                .consumer(consumerSpy)
                .maxConcurrency(maxConcurrency)
                .build();
    }

    private void addRecordsWithSetKey(final int numRecords) {
        long recordSetKey = RECORD_SET_KEY_GENERATOR.incrementAndGet();
        log.debug("Producing {} records with set key {}.", numRecords, recordSetKey);
        for (int i = 0; i < numRecords; ++i) {
            consumerSpy.addRecord(ktu.makeRecord("key-" + recordSetKey + i, "v0-test-" + i));
        }
        log.debug("Finished producing {} records with set key {}.", numRecords, recordSetKey);
    }

    private void setupParallelConsumerInstance(final CommitMode commitMode, final int maxConcurrency) {
        setupParallelConsumerInstance(getBaseOptions(commitMode, maxConcurrency));

        // register unique ID on the parallel consumer
        String myId = "p/r-test-" + MY_ID_GENERATOR.incrementAndGet();
        parallelConsumer.setMyId(Optional.of(myId));
    }

    private TestUserFunction createTestSetup(final CommitMode commitMode, final int maxConcurrency) {
        setupParallelConsumerInstance(commitMode, maxConcurrency);
        TestUserFunction testUserFunction = new TestUserFunction();
        parallelConsumer.poll(testUserFunction);

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
        int numTestRecordsPerSet = 1_000;
        int totalRecordsExpected = 2 * numTestRecordsPerSet;

        TestUserFunction testUserFunction = createTestSetup(commitMode, 3);

        // produce some messages
        addRecordsWithSetKey(numTestRecordsPerSet);

        // wait for processing to finish
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(numTestRecordsPerSet + " records should be processed")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(numTestRecordsPerSet));

        // overall committed offset should reach the same value
        awaitForCommit(numTestRecordsPerSet);

        //
        testUserFunction.reset();

        // pause parallel consumer and wait for control loops to catch up
        parallelConsumer.pauseIfRunning();

        awaitForOneLoopCycle();

        // produce more messages -> nothing should be processed
        addRecordsWithSetKey(numTestRecordsPerSet);

        awaitForSomeLoopCycles(2);

        // shouldn't have produced any records
        assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(0L);

        // overall committed offset should stay at old value
        awaitForCommit(numTestRecordsPerSet);

        // resume parallel consumer ->
        parallelConsumer.resumeIfPaused();

        // messages should be processed now
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(numTestRecordsPerSet + " records should be processed")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(numTestRecordsPerSet));

        // overall committed offset should reach the total of two batches that were processed
        awaitForCommit(totalRecordsExpected);
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
        addRecordsWithSetKey(numTestRecordsPerBatch);

        // wait until we have enough records in flight
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(degreeOfParallelism + " records should be in flight processed")
                .untilAsserted(() -> assertThat(testUserFunction.numInFlightRecords.get()).isEqualTo(degreeOfParallelism));

        //
        assertCommits().isEmpty();

        // pause parallel consumer and wait for control loops to catch up
        parallelConsumer.pauseIfRunning();
        awaitForOneLoopCycle();

        // unlock the user function
        testUserFunction.unlockProcessing();

        // in flight messages + buffered messages should get processed now (exact number is based on dynamic load factor)
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias("at least " + degreeOfParallelism + " records should be processed")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isGreaterThan(degreeOfParallelism));

        // overall committed offset should reach the same value
        awaitForCommit(testUserFunction.numProcessedRecords.get());

        // shouldn't have anymore in flight records now
        assertThat(testUserFunction.numInFlightRecords.get()).isEqualTo(0);
        assertThat(parallelConsumer.getWm().getNumberRecordsOutForProcessing()).isEqualTo(0);

        // resume parallel consumer ->
        parallelConsumer.resumeIfPaused();

        // other pending messages should be processed now
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(numTestRecordsPerBatch + " records should be processed")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(numTestRecordsPerBatch));

        // overall committed offset should reach the total number of processed records
        awaitForCommit(testUserFunction.numProcessedRecords.get());
    }

}
