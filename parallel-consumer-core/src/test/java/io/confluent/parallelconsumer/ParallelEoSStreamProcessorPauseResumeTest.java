package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;

import com.google.common.truth.Truth;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.filter.LevelFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.spi.FilterReply;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Test for pause/resume feature of the parallel consumer (see {@code GH#193}).
 *
 * @author niels.oertel
 */
@Timeout(value = 10, unit = SECONDS)
@Slf4j
class ParallelEoSStreamProcessorPauseResumeTest extends ParallelEoSStreamProcessorTestBase {

    private static final AtomicLong MY_ID_GENERATOR = new AtomicLong();

    private static final AtomicLong RECORD_BATCH_KEY_GENERATOR = new AtomicLong();

    private ControlLoopTracker controlLoopTracker;

    private static class TestUserFunction implements Consumer<ConsumerRecord<String, String>> {

        private final AtomicLong numProcessedRecords = new AtomicLong();

        /**
         * The number of in flight records. Note that this may not exactly match the
         * real number of in flight records as parallel consumer has a wrapper around
         * the user function so incrementing/decrementing the counter is a little bit
         * delayed.
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
    }

    /**
     * Log message appender that monitors the entry messages of the two control
     * loops and counts how often they have been executed to build reliable tests
     * that don't need to use {@link Thread#sleep(long)} to wait for changes to take
     * effect.
     * <p>
     * Note: {@link ParallelEoSStreamProcessorTestBase#waitForSomeLoopCycles(int)}
     * provides similar functionality but not for the {@link BroerPollSystem}.
     * </p>
     *
     * @author niels.oertel
     */
    public static class ControlLoopTracker extends AppenderBase<ILoggingEvent> {

        private static final AtomicLong ID_GENERATOR = new AtomicLong();

        private static final String PSP_CONTROL_LOOP_MESSAGE = "Loop: Process mailbox";

        private static final String BPS_CONTROL_LOOP_MESSAGE = "Loop: Broker poller: ({})";

        private final String myInstanceId;

        private final AtomicLong pspControlLoopCounter = new AtomicLong();

        private final AtomicLong bpsControlLoopCounter = new AtomicLong();

        public ControlLoopTracker(final String myInstanceId) {
            this.myInstanceId = myInstanceId;
            this.setName("ControlLoopMonitor-" + ID_GENERATOR.incrementAndGet());
        }

        @Override
        protected void append(final ILoggingEvent e) {
            String myInstanceId = e.getMDCPropertyMap().get(AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID);
            if (!this.myInstanceId.equals(myInstanceId)) {
                // log message doesn't belong to the parallel consumer that is tracked
            } else if (AbstractParallelEoSStreamProcessor.class.getName().equals(e.getLoggerName())
                    && PSP_CONTROL_LOOP_MESSAGE.equals(e.getMessage())) {
                // this is a control loop message from the parallel stream processor
                pspControlLoopCounter.incrementAndGet();
            } else if (BrokerPollSystem.class.getName().equals(e.getLoggerName())
                    && BPS_CONTROL_LOOP_MESSAGE.equals(e.getMessage())) {
                // this is a control loop message from the broker poll system
                bpsControlLoopCounter.incrementAndGet();
            } else {
                // this is a message from the parallel consumer that is tracked but we're not
                // interested in it
            }
        }

        public void reset() {
            this.pspControlLoopCounter.set(0L);
            this.bpsControlLoopCounter.set(0L);
        }

        public void waitForSomeControlLoopCycles(int numCycles, long timeout, TimeUnit unit) {
            long currentPspControlLoopCounter = pspControlLoopCounter.get();
            long currentBspControlLoopCounter = bpsControlLoopCounter.get();
            waitForSomeLoopCycles(currentPspControlLoopCounter, pspControlLoopCounter::get, numCycles, timeout, unit);
            waitForSomeLoopCycles(currentBspControlLoopCounter, bpsControlLoopCounter::get, numCycles, timeout, unit);
        }

        public void waitForSomeParallelStreamProcessorControlLoopCycles(int numCycles, long timeout, TimeUnit unit) {
            waitForSomeLoopCycles(pspControlLoopCounter.get(), pspControlLoopCounter::get, numCycles, timeout, unit);
        }

        public void waitForSomeBrokerPollSystemControlLoopCycles(int numCycles, long timeout, TimeUnit unit) {
            waitForSomeLoopCycles(bpsControlLoopCounter.get(), bpsControlLoopCounter::get, numCycles, timeout, unit);
        }

        private static void waitForSomeLoopCycles(long lastCount, Supplier<Long> currentCounter, int numCycles,
                long timeout, TimeUnit unit) {
            Awaitility.waitAtMost(timeout, unit).alias("Hello world: " + lastCount).until(currentCounter::get,
                    currentCount -> currentCount >= lastCount + numCycles);
        }
    }

    @AfterEach
    void cleanup() {
        if (null != controlLoopTracker) {
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            lc.getLoggerList().forEach(logger -> logger.detachAppender(controlLoopTracker.getName()));
            controlLoopTracker.reset();
            controlLoopTracker.stop();
            controlLoopTracker = null;
        }

        if (null != parallelConsumer) {
            parallelConsumer.close();
        }
    }

    private void setupParallelConsumerInstanceAndLogCapture(final CommitMode commitMode, final int maxConcurrency) {
        setupParallelConsumerInstance(getBaseOptions(commitMode, maxConcurrency));

        // register unique ID on the parallel consumer
        String myId = "p/r-test-" + MY_ID_GENERATOR.incrementAndGet();
        parallelConsumer.setMyId(Optional.of(myId));

        // setup the log capture to be able to follow the two loops
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();

        // add filter to the STDOUT appender to ensure it stays on level INFO
        LevelFilter levelInfoFilter = new LevelFilter();
        levelInfoFilter.setContext(lc);
        levelInfoFilter.setLevel(Level.INFO);
        levelInfoFilter.setOnMismatch(FilterReply.DENY);
        levelInfoFilter.start();
        lc.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("STDOUT").addFilter(levelInfoFilter);

        // create control loop tracker and register it as log appender
        controlLoopTracker = new ControlLoopTracker(myId);
        controlLoopTracker.setContext(lc);
        Logger parallelStreamProcessorLogger = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);
        parallelStreamProcessorLogger.setLevel(Level.TRACE);
        parallelStreamProcessorLogger.addAppender(controlLoopTracker);
        Logger brokerPollSystemLogger = (Logger) LoggerFactory.getLogger(BrokerPollSystem.class);
        brokerPollSystemLogger.setLevel(Level.TRACE);
        brokerPollSystemLogger.addAppender(controlLoopTracker);
        controlLoopTracker.start();
    }

    private long getOverallCommittedOffset() {
        return getCommittedOffsetsByPartitions().values().stream().collect(Collectors.summingLong(Long::longValue));
    }

    private Map<TopicPartition, Long> getCommittedOffsetsByPartitions() {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistory = getCommitHistory();
        if (commitHistory.isEmpty()) {
            return Collections.emptyMap();
        }

        Set<String> consumerGroups = commitHistory.stream().flatMap(c -> c.keySet().stream())
                .collect(Collectors.toSet());
        // verify that test setup is correct (this method only supports a single consumer group for now)
        Truth.assertThat(consumerGroups).hasSize(1);
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
        Truth.assertThat(getOverallCommittedOffset()).isEqualTo(0L);

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

        TestUserFunction testUserFunction = createTestSetup(commitMode, 3);

        // produce some messages
        addRecords(numTestRecordsPerBatch);

        // wait for processing to finish
        Awaitility
            .waitAtMost(5L, TimeUnit.SECONDS)
            .pollDelay(50L, TimeUnit.MILLISECONDS)
            .alias(numTestRecordsPerBatch + " records should be processed")
            .until(testUserFunction.numProcessedRecords::get, numRecords -> numTestRecordsPerBatch == numRecords);
        // overall committed offset should reach the same value
        Awaitility
            .waitAtMost(5L, TimeUnit.SECONDS)
            .alias("sum of consumer offsets should reach " + numTestRecordsPerBatch)
            .until(this::getOverallCommittedOffset, numRecords -> numTestRecordsPerBatch == numRecords);
        testUserFunction.reset();

        // pause parallel consumer and wait for control loops to catch up
        parallelConsumer.pauseIfRunning();
        controlLoopTracker.waitForSomeParallelStreamProcessorControlLoopCycles(1, 5L, TimeUnit.SECONDS);
        controlLoopTracker.waitForSomeBrokerPollSystemControlLoopCycles(1, 5L, TimeUnit.SECONDS);

        // produce more messages -> nothing should be processed
        addRecords(numTestRecordsPerBatch);
        controlLoopTracker.waitForSomeControlLoopCycles(5, 5L, TimeUnit.SECONDS);

        // shouldn't have produced any records
        Truth.assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(0L);

        // overall committed offset should stay at old value
        Truth.assertThat(getOverallCommittedOffset()).isEqualTo(numTestRecordsPerBatch);

        // resume parallel consumer -> messages should be processed now
        parallelConsumer.resumeIfPaused();
        Awaitility
            .waitAtMost(5L, TimeUnit.SECONDS)
            .alias(numTestRecordsPerBatch + " records should be processed")
            .until(testUserFunction.numProcessedRecords::get, numRecords -> numTestRecordsPerBatch == numRecords);
        // overall committed offset should reach the total of two batches that were processed
        Awaitility
            .waitAtMost(5L, TimeUnit.SECONDS)
            .alias("sum of consumer offsets should reach " + 2L * numTestRecordsPerBatch)
            .until(this::getOverallCommittedOffset, numRecords -> 2L * numTestRecordsPerBatch == numRecords);
        testUserFunction.reset();
    }

    /**
     * This test verifies that in flight work is finished successfully when the consumer is paused. In flight work is
     * work that's currently being processed inside a user function has already been submitted to be processed based
     * on the dynamic load factor.
     * The test also verifies that new offsets are committed once the in-flight work finishes even if the consumer is
     * still paused.
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
            .waitAtMost(5L, TimeUnit.SECONDS)
            .pollDelay(50L, TimeUnit.MILLISECONDS)
            .alias(degreeOfParallelism + " records should be in flight processed")
            .until(testUserFunction.numInFlightRecords::get, numInFlightRecords -> degreeOfParallelism == numInFlightRecords);

        // overall committed consumer offset should still be at 0
        Truth.assertThat(getOverallCommittedOffset()).isEqualTo(0L);

        // pause parallel consumer and wait for control loops to catch up
        parallelConsumer.pauseIfRunning();
        controlLoopTracker.waitForSomeParallelStreamProcessorControlLoopCycles(1, 5L, TimeUnit.SECONDS);
        controlLoopTracker.waitForSomeBrokerPollSystemControlLoopCycles(1, 5L, TimeUnit.SECONDS);

        // unlock the user function
        testUserFunction.unlockProcessing();

        // in flight messages + buffered messages should get processed now (exact number is based on dynamic load factor)
        Awaitility
            .waitAtMost(5L, TimeUnit.SECONDS)
            .pollDelay(50L, TimeUnit.MILLISECONDS)
            .alias("at least " + degreeOfParallelism + " records should be processed")
            .until(testUserFunction.numProcessedRecords::get, numRecords -> degreeOfParallelism <= numRecords);
        // overall committed offset should reach the same value
        Awaitility
            .waitAtMost(5L, TimeUnit.SECONDS)
            .alias("sum of consumer offsets should reach number of processed records")
            .until(this::getOverallCommittedOffset, numRecords -> testUserFunction.numProcessedRecords.get() == numRecords);
        // shouldn't have any more in flight records now
        Truth.assertThat(testUserFunction.numInFlightRecords.get()).isEqualTo(0);

        // resume parallel consumer -> other pending messages should be processed now
        parallelConsumer.resumeIfPaused();
        Awaitility
            .waitAtMost(5L, TimeUnit.SECONDS)
            .alias(numTestRecordsPerBatch + " records should be processed")
            .until(testUserFunction.numProcessedRecords::get, numRecords -> numTestRecordsPerBatch == numRecords);
        // overall committed offset should reach the total number of processed records
        Awaitility
            .waitAtMost(5L, TimeUnit.SECONDS)
            .alias("sum of consumer offsets should reach number of processed records")
            .until(this::getOverallCommittedOffset, numRecords -> testUserFunction.numProcessedRecords.get() == numRecords);
        testUserFunction.reset();
    }

    // TODO: Add also test for pausing under load
}
