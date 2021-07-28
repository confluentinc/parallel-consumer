package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.utils.LatchTestUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static io.confluent.csid.utils.GeneralTestUtils.time;
import static io.confluent.csid.utils.KafkaUtils.toTP;
import static io.confluent.csid.utils.LatchTestUtils.awaitLatch;
import static io.confluent.csid.utils.LatchTestUtils.constructLatches;
import static io.confluent.csid.utils.Range.range;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.*;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static pl.tlinkowski.unij.api.UniLists.of;

@Timeout(value = 10, unit = SECONDS)
@Slf4j
public class ParallelEoSStreamProcessorTest extends ParallelEoSStreamProcessorTestBase {

    public static class MyAction implements Function<ConsumerRecord<String, String>, String> {

        @Override
        public String apply(ConsumerRecord<String, String> record) {
            log.info("User client function - consuming a record... {}", record.key());
            return "my-result";
        }
    }

    @BeforeEach()
    public void setupData() {
        primeFirstRecord();
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    public void failingActionNothingCommitted(CommitMode commitMode) {
        setupParallelConsumerInstance(commitMode);

        parallelConsumer.poll((ignore) -> {
            throw new RuntimeException("My user's function error");
        });

        // let it process
        waitForSomeLoopCycles(3);

        parallelConsumer.close();

        //
        assertCommits(of(), "All erroring, so nothing committed except initial");
    }

    /**
     * Checks that for messages that are currently undergoing processing, that no offsets for them are committed
     */
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void offsetsAreNeverCommittedForMessagesStillInFlightSimplest(CommitMode commitMode) {
        var options = getBaseOptions(commitMode).toBuilder()
                .ordering(UNORDERED)
                .build();
        setupParallelConsumerInstance(options);

        primeFirstRecord();
        sendSecondRecord(consumerSpy);

        assertThat(parallelConsumer.getWm().getOptions().getOrdering()).isEqualTo(UNORDERED);

        var locks = constructLatches(2);

        var processedStates = new LinkedHashMap<Integer, Boolean>();

        var startBarrierLatch = new CountDownLatch(1);

        // finish processing only msg 1
        parallelConsumer.poll((ignore) -> {
            startBarrierLatch.countDown();
            int offset = (int) ignore.offset();
            LatchTestUtils.awaitLatch(locks, offset);
            processedStates.put(offset, true);
        });

        awaitLatch(startBarrierLatch);

        assertThat(parallelConsumer.getWm().getTotalWorkWaitingProcessing()).isEqualTo(2);

        assertThat(parallelConsumer.getWm().getNumberRecordsOutForProcessing()).isEqualTo(2);

        // finish processing 1
        releaseAndWait(locks, 1);

        parallelConsumer.requestCommitAsap();

        // make sure no offsets are committed
        assertCommits(of(), "Partition is blocked");

        // So it's data is setup can be used in other tests, finish 0
        releaseAndWait(locks, 0);

        parallelConsumer.requestCommitAsap();
        waitForSomeLoopCycles(5);

        log.debug("Closing...");
        parallelConsumer.close();

        assertThat(processedStates)
                .as("sanity - all expected messages are processed")
                .containsValues(true, true);
    }

    private void setupParallelConsumerInstance(final CommitMode commitMode) {
        setupParallelConsumerInstance(getBaseOptions(commitMode));
        // created a new client above, so have to send the prime record again
        primeFirstRecord();
    }

    private ParallelConsumerOptions getBaseOptions(final CommitMode commitMode) {
        return ParallelConsumerOptions.<String, String>builder()
                .commitMode(commitMode)
                .consumer(consumerSpy)
                .producer(producerSpy)
                .build();
    }

    /**
     * {@link #offsetsAreNeverCommittedForMessagesStillInFlightSimplest(CommitMode)} doesnt check the final offsets -
     * that's what this test does.
     *
     * @param commitMode
     */
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void offsetsAreNeverCommittedForMessagesStillInFlightShort(CommitMode commitMode) {
        offsetsAreNeverCommittedForMessagesStillInFlightSimplest(commitMode);
        log.info("Test start");

        // next expected offset is now 2
        await().untilAsserted(() ->
                assertCommits(of(2), "Only one of the two offsets committed, as they were coalesced for efficiency"));
    }

    @Disabled
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    void offsetsAreNeverCommittedForMessagesStillInFlightLong(CommitMode commitMode) {
        setupParallelConsumerInstance(commitMode);

        sendSecondRecord(consumerSpy);

        // send three messages - 0, 1, 2
        consumerSpy.addRecord(ktu.makeRecord("0", "v2"));
        consumerSpy.addRecord(ktu.makeRecord("0", "v3"));
        consumerSpy.addRecord(ktu.makeRecord("0", "v4"));
        consumerSpy.addRecord(ktu.makeRecord("0", "v5"));

        List<CountDownLatch> locks = constructLatches(6);

        CountDownLatch startLatch = new CountDownLatch(1);

        parallelConsumer.poll((ignore) -> {
            int offset = (int) ignore.offset();
            CountDownLatch latchForMsg = locks.get(offset);
            try {
                startLatch.countDown();
                latchForMsg.await();
            } catch (InterruptedException e) {
                // ignore
            }
        });

        startLatch.countDown();

        // finish processing 1
        releaseAndWait(locks, 1);

        waitForSomeLoopCycles(1);

        // make sure no offsets are committed
        verify(producerSpy, after(verificationWaitDelay).never()).commitTransaction();

        // finish 2
        releaseAndWait(locks, 2);

        //
        waitForSomeLoopCycles(1);

        // make sure no offsets are committed
        verify(producerSpy, after(verificationWaitDelay).never()).commitTransaction();

        // finish 0
        releaseAndWait(locks, 0);
        waitForOneLoopCycle();

        // make sure offset 2, not 0 or 1 is committed
        verify(producerSpy, after(verificationWaitDelay).times(1)).commitTransaction();
        var maps = producerSpy.consumerGroupOffsetsHistory();
        assertThat(maps).hasSize(1);
        OffsetAndMetadata offsets = maps.get(0).get(CONSUMER_GROUP_ID).get(toTP(firstRecord));
        assertThat(offsets.offset()).isEqualTo(2);

        // finish 3
        releaseAndWait(locks, 3);

        // 3 committed
        verify(producerSpy, after(verificationWaitDelay).times(2)).commitTransaction();
        maps = producerSpy.consumerGroupOffsetsHistory();
        assertThat(maps).hasSize(2);
        offsets = maps.get(1).get(CONSUMER_GROUP_ID).get(toTP(firstRecord));
        assertThat(offsets.offset()).isEqualTo(3);

        // finish 4,5
        releaseAndWait(locks, of(4, 5));

        // 5 committed
        verify(producerSpy, after(verificationWaitDelay).atLeast(3)).commitTransaction();
        maps = producerSpy.consumerGroupOffsetsHistory();
        assertThat(maps).hasSizeGreaterThanOrEqualTo(3);
        offsets = maps.get(2).get(CONSUMER_GROUP_ID).get(toTP(firstRecord));
        assertThat(offsets.offset()).isEqualTo(5);
        assertCommits(of(2, 3, 5));

        // close
        parallelConsumer.close();
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void offsetCommitsAreIsolatedPerPartition(CommitMode commitMode) {
        // Disable this test for vert.x for now
        Assumptions.assumeThat(parallelConsumer)
                .as("Should only test on core PC - this test is very complicated to get to work with vert.x " +
                        "thread system, as the event and locking system needed is quite different")
                .isExactlyInstanceOf(AbstractParallelEoSStreamProcessor.class);

        setupParallelConsumerInstance(getBaseOptions(commitMode).toBuilder()
                .ordering(UNORDERED)
                .build());
        primeFirstRecord();

        sendSecondRecord(consumerSpy);

        // send messages - 0,1, to one partition and 3,4 to another partition petitions
        consumerSpy.addRecord(ktu.makeRecord(1, "0", "v2"));
        consumerSpy.addRecord(ktu.makeRecord(1, "0", "v3"));

        var msg0Lock = new CountDownLatch(1);
        var msg1Lock = new CountDownLatch(1);
        var msg2Lock = new CountDownLatch(1);
        var msg3Lock = new CountDownLatch(1);

        List<CountDownLatch> locks = of(msg0Lock, msg1Lock, msg2Lock, msg3Lock);

        parallelConsumer.poll((ignore) -> {
            int offset = (int) ignore.offset();
            CountDownLatch latchForMsg = locks.get(offset);
            try {
                latchForMsg.await();
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
        });

        // finish processing 1
        releaseAndWait(locks, 1);

        parallelConsumer.requestCommitAsap();

        waitForSomeLoopCycles(50); // async commit can be slow - todo change this to event based

        // make sure only base offsets are committed for partition (next expected = 0 and 2 respectively)
//        assertCommits(of(2));
        assertCommitLists(of(of(), of(2)));

        // finish 2
        releaseAndWait(locks, 2);
        parallelConsumer.requestCommitAsap();

        // make sure only 2 on it's partition is committed
//        assertCommits(of(2, 3));
        await().untilAsserted(() ->
                assertCommitLists(of(of(), of(2, 3))));

        // finish 0
        releaseAndWait(locks, 0);

        parallelConsumer.requestCommitAsap();

        waitForOneLoopCycle();
        if (isUsingAsyncCommits())
            waitForSomeLoopCycles(3); // async commit can be slow - todo change this to event based

        // make sure offset 0 and 1 is committed
        assertCommitLists(of(of(2), of(2, 3)));

        // finish 3
        releaseAndWait(locks, 3);

        // async consumer is slower to execute the commit. We could just wait, or we could add an event to the async consumer commit cycle
        if (isUsingAsyncCommits())
            waitForSomeLoopCycles(3); // async commit can be slow - todo change this to event based

        //
        await().untilAsserted(() ->
                assertCommitLists(of(of(2), of(2, 3, 4))));
    }

    @Test
    @Disabled
    public void avro() {
        // send three messages - 0,1,2
        // finish processing 1
        // make sure no offsets are committed
        // finish 0
        // make sure offset 1, not 0 is committed
        assertThat(false).isTrue();
    }

    @ParameterizedTest
    @EnumSource(CommitMode.class)
    void controlFlowException(CommitMode commitMode) {
        // setup again manually to use subscribe instead of assign (for revoke testing)
        instantiateConsumerProducer();
        parentParallelConsumer = initPollingAsyncConsumer(getBaseOptions(commitMode));
        subscribeParallelConsumerAndMockConsumerTo(INPUT_TOPIC);
        setupData();

        // cause a control loop error
        parallelConsumer.addLoopEndCallBack(() -> {
            throw new FakeRuntimeError("My fake control loop error");
        });

        //
        parallelConsumer.poll((ignore) -> {
            log.info("Ignoring {}", ignore);
        });

        // close and retrieve exception in control loop
        assertThatThrownBy(() -> {
            parallelConsumer.closeDrainFirst(ofSeconds(10));
        }).hasMessageContainingAll("Error", "poll", "thread", "fake control");
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    public void testVoid(CommitMode commitMode) {
        setupParallelConsumerInstance(commitMode);

        int expected = 1;
        var msgCompleteBarrier = new CountDownLatch(expected);
        parallelConsumer.poll((record) -> {
            myRecordProcessingAction.apply(record);
            msgCompleteBarrier.countDown();
        });

        awaitLatch(msgCompleteBarrier);

        waitForSomeLoopCycles(1);

        parallelConsumer.close();

        assertCommits(of(1));

        verify(myRecordProcessingAction, times(expected)).apply(any());

        // assert internal methods - shouldn't really need this as we already check the commit history above through the
        // spy, so can leave in for the old producer style
        if (commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER)) {
            verify(producerSpy, atLeastOnce()).commitTransaction();
            verify(producerSpy, atLeastOnce()).sendOffsetsToTransaction(anyMap(), ArgumentMatchers.<ConsumerGroupMetadata>any());
        }
    }

    @Test
    @Disabled
    public void userSucceedsButProduceToBrokerFails() {
    }

    @Test
    @Disabled
    public void poisonPillGoesToDeadLetterQueue() {
    }

    @Test
    @Disabled
    public void failingMessagesDontBreakCommitOrders() {
        assertThat(false).isTrue();
    }

    @Test
    @Disabled
    public void messagesCanBeProcessedOptionallyPartitionOffsetOrder() {
    }

    @Test
    @Disabled
    public void failingMessagesThatAreRetriedDontBreakProcessingOrders() {
        assertThat(false).isTrue();
    }

    @Test
    @Disabled
    public void ifTooManyMessagesAreInFlightDontPollBrokerForMore() {
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    @Disabled
    public void processInKeyOrder(CommitMode commitMode) {
        setupParallelConsumerInstance(ParallelConsumerOptions.builder()
                .commitMode(commitMode)
                .ordering(KEY)
                .build());
        // created a new client above, so have to send the prime record again
        primeFirstRecord();

        // sanity check
        assertThat(parallelConsumer.getWm().getOptions().getOrdering()).isEqualTo(KEY);

        sendSecondRecord(consumerSpy);

        // 0,1 previously sent to partition 0
        // send two more to part 0 - 2,3,
        consumerSpy.addRecord(ktu.makeRecord("key-1", "v2")); // 2
        consumerSpy.addRecord(ktu.makeRecord("key-1", "v3")); // 3

        // and 3,4 to another partition
        consumerSpy.addRecord(ktu.makeRecord(1, "key-2", "v4")); // 4
        consumerSpy.addRecord(ktu.makeRecord(1, "key-3", "v5")); // 5
        consumerSpy.addRecord(ktu.makeRecord(1, "key-3", "v6")); // 6
        consumerSpy.addRecord(ktu.makeRecord(1, "key-3", "v7")); // 7
        consumerSpy.addRecord(ktu.makeRecord(1, "key-4", "v8")); // 8 - 8 must not get committed before 7 does

        // so 3 and 4 will block each other only
        // and 0,1,2,3 will all block each other (part 0)

        // if we're going to block 8 threads, need a big enough executor pool
        var msg0Lock = new CountDownLatch(1);
        var msg1Lock = new CountDownLatch(1);
        var msg2Lock = new CountDownLatch(1);
        var msg3Lock = new CountDownLatch(1);
        var msg4Lock = new CountDownLatch(1);
        var msg5Lock = new CountDownLatch(1);
        var msg6Lock = new CountDownLatch(1);
        var msg7Lock = new CountDownLatch(1);
        var msg8Lock = new CountDownLatch(1);

        final var processedState = new HashMap<Integer, Boolean>();
        for (Integer msgIndex : range(8)) {
            processedState.put(msgIndex, false);
        }

        List<CountDownLatch> locks = of(msg0Lock, msg1Lock, msg2Lock, msg3Lock, msg4Lock, msg5Lock, msg6Lock, msg7Lock, msg8Lock);

        final List polled = new ArrayList();
        Mockito.doAnswer(x -> {
            ConsumerRecords o = (ConsumerRecords) x.callRealMethod();
            for (Object o1 : o) {
                polled.add(o1);
            }
            return o;
        }).when(consumerSpy).poll(any());

        parallelConsumer.poll((ignore) -> {
            int offset = (int) ignore.offset();
            CountDownLatch latchForMsg = locks.get(offset);
            try {
                log.debug("Started msg {} processing, locking on latch to simulate long process times...", offset);
                latchForMsg.await();
            } catch (InterruptedException e) {
                // ignore
            }
            log.debug("Finished msg {} processing after waking...", offset);
            processedState.put(offset, true);
        });

        // Finish these immediately
        msg6Lock.countDown();
        msg8Lock.countDown();

        // unlock 1
        log.debug("Unlocking 1...");
        msg1Lock.countDown();

        // wait cycles to make sure
        waitForOneLoopCycle();

        //
        assertThat(polled).as("sanity check input data").hasSameSizeAs(locks);

        //
        assertThat(processedState.get(1))
                .as("blocked by 0 (1 shouldn't be run until 0 is complete, due to key order processing)")
                .isFalse();

        // make sure no offsets are committed
        assertCommits(of());

        // finish 2 process clear, but commit blocked by 0
        log.debug("Unlocking 2...");
        msg2Lock.countDown();
        waitForSomeLoopCycles(2);
        assertThat(processedState.get(2)).isTrue();


        // still nothing - 0 blocks 1 and 2 (partition 0)
        verify(producerSpy, after(verificationWaitDelay).never()).commitTransaction(); // todo remove all wait nevers in favour of triggers as it slows down test
        waitForOneLoopCycle();
        assertCommits(of());

        // finish 0 - releases pending (1,2)
        log.debug("Unlocking 0...");
        msg0Lock.countDown();

        // 0 gets comitted by itself
        waitForCommitExact(0, 0);

        // make sure offset 0 is committed. 1 is now free to be processed (same key as 0), which as 2 was processed previously, frees up offset 2 to commit
        waitForCommitExact(0, 2);
        assertCommits(of(0, 2));

        // unlock 3 - should get committed
        log.debug("Unlocking 3...");
        msg3Lock.countDown();

        // unlock 5 - commit blocked by 4, but should finish processing and clear 6 and then 7 (in 2 loops) for processing
        log.debug("Unlocking 5...");
        msg5Lock.countDown();
        waitUntilTrue(() -> processedState.get(5));
        assertThat(processedState.get(5)).as("5 should processed").isTrue();

        waitForCommitExact(0, 3);
        assertCommits(of(0, 2, 3));

        // unlock 4 - clears 5 for offset commit - 7 not processed yet (5,6,7 same key), 8 was never locked
        log.debug("Unlocking 4...");
        msg4Lock.countDown();

        // 6 should have been processed, unblocked by 5 (same key)
        waitUntilTrue(() -> processedState.get(6));
        assertThat(processedState.get(6)).as("6 should processed").isTrue();

        // 5 and 6 finished, same key, coalesced commit to 6
        waitForSomeLoopCycles(1);
        waitForCommitExact(1, 6);
        assertCommits(of(0, 2, 3, 6));

        // unlock 7 (same key as 6), unblocks 8 for commit
        assertThat(processedState.get(7)).isFalse();
        assertThat(processedState.get(8)).isTrue();
        //
        releaseAndWait(locks, 7);
        waitForCommitExact(1, 8);
        assertCommits(of(0, 2, 3, 6, 8));
    }

    /**
     * Check that when processing in key order, when work is not completed or taking a long time, that the commit system
     * doesn't break.
     */
    @SneakyThrows
    @Test
    void processInKeyOrderWorkNotReturnedDoesntBreakCommits() {
        ParallelConsumerOptions options = ParallelConsumerOptions.builder()
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .ordering(KEY)
                .build();
        setupParallelConsumerInstance(options);
        primeFirstRecord();

        sendSecondRecord(consumerSpy);

        // sanity check
        assertThat(parallelConsumer.getWm().getOptions().getOrdering()).isEqualTo(KEY);

        // 0,1 previously sent to partition 0
        // send one more, with same key of 1
        consumerSpy.addRecord(ktu.makeRecord("key-1", "v2")); // 2

        CountDownLatch msg1latch = new CountDownLatch(1);
        HashMap<Integer, CountDownLatch> locks = new HashMap<>();
        locks.put(1, msg1latch);

        CountDownLatch twoLoopLatch = new CountDownLatch(2);
        CountDownLatch fourLoopLatch = new CountDownLatch(4);
        parallelConsumer.addLoopEndCallBack(() -> {
            log.trace("Control loop cycle - {}, {}", twoLoopLatch.getCount(), fourLoopLatch.getCount());
            twoLoopLatch.countDown();
            fourLoopLatch.countDown();
        });

        var polled = new ArrayList<>();
        doAnswer(x -> {
            var records = (ConsumerRecords<String, String>) x.callRealMethod();
            for (var record : records) {
                polled.add(record);
            }
            return records;
        }).when(consumerSpy).poll(any());

        parallelConsumer.poll((ignore) -> {
            int offset = (int) ignore.offset();
            CountDownLatch countDownLatch = locks.get(offset);
            if (countDownLatch != null) try {
                countDownLatch.await();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            log.debug("Message offset {} processed...", offset);
        });

        await().untilAsserted(() ->
                assertThat(polled)
                        .as("sanity check - the records have been polled")
                        .hasSize(3)
        );

        //
        awaitLatch(twoLoopLatch);
        waitForOneLoopCycle();

        //
        await().untilAsserted(() -> {
            try {
                // simpler way of making the bootstrap commit optional in the results, than adding the required barrier
                // locks to ensure it's existence, which has been tested else where
                assertCommits(of(0, 1), "Only 0 should be committed, as even though 2 is also finished, 1 should be " +
                        "blocking the partition");
            } catch (AssertionError e) {
                assertCommits(of(1), "Bootstrap commit is optional. See msg in code above");
            }
        });

        //
        msg1latch.countDown(); // release remaining processing lock

        //
        awaitLatch(fourLoopLatch); // wait for some loops

        // one more step
        waitForOneLoopCycle();

        await().untilAsserted(() -> {
            //
            try { // see above
                assertCommits(of(0, 1, 3), "Remaining two records should be committed as a single offset");
            } catch (AssertionError e) {
                assertCommits(of(1, 3), "Bootstrap commit is optional. See msg in code above");
            }
        });
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    public void closeAfterSingleMessageShouldBeEventBasedFast(CommitMode commitMode) {
        setupParallelConsumerInstance(commitMode);

        var msgCompleteBarrier = new CountDownLatch(1);

        parallelConsumer.poll((ignore) -> {
            log.info("Message processed: {} - noop", ignore.offset());
            msgCompleteBarrier.countDown();
        });

        awaitLatch(msgCompleteBarrier);

        // allow for offset to be committed
        waitForOneLoopCycle();

        parallelConsumer.requestCommitAsap();

        waitForOneLoopCycle();

        await().untilAsserted(() ->
                assertCommits(of(1)));

        // close
        Duration time = time(() -> {
            parallelConsumer.close();
        });

        //
        assertThat(time).as("Should not be blocked for any reason")
                .isLessThan(ofSeconds(1));
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    public void closeWithoutRunningShouldBeEventBasedFast(CommitMode commitMode) {
        setupParallelConsumerInstance(getBaseOptions(commitMode));

        parallelConsumer.closeDontDrainFirst();
    }

    @Test
    public void ensureLibraryCantBeUsedTwice() {
        parallelConsumer.poll(ignore -> {
        });
        assertThatIllegalStateException().isThrownBy(() -> {
            parallelConsumer.poll(ignore -> {
            });
        });
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    void consumeFlowDoesntRequireProducer(CommitMode commitMode) {
        setupClients();

        var optionsWithClients = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerSpy)
                .commitMode(commitMode)
                .build();

        if (commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER)) {
            assertThatThrownBy(() -> parallelConsumer = initPollingAsyncConsumer(optionsWithClients))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContainingAll("Producer", "Transaction");
        } else {
            parallelConsumer = initPollingAsyncConsumer(optionsWithClients);
            attachLoopCounter(parallelConsumer);

            subscribeParallelConsumerAndMockConsumerTo(INPUT_TOPIC);
            setupData();

            parallelConsumer.poll((ignore) -> {
                log.debug("Test record processor - rec: {}", ignore);
            });

            //
            parallelConsumer.requestCommitAsap();
            waitForCommitExact(1);

            parallelConsumer.closeDrainFirst();

            //
            assertCommits(of(1));
        }
    }

    @Test
    void optionsProduceMessageFlowRequiresProducer() {
        setupClients();

        var optionsWithClients = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerSpy)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        assertThatThrownBy(() -> parallelConsumer = initPollingAsyncConsumer(optionsWithClients))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Producer", "Transaction");
    }


    @Test
    void optionsGroupIdRequiredAndAutoCommitDisabled() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Deserializer<String> deserializer = Serdes.String().deserializer();
        var realConsumer = new KafkaConsumer<>(properties, deserializer, deserializer);

        var optionsBuilder = ParallelConsumerOptions.<String, String>builder()
                .consumer(realConsumer)
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS);
        var optionsWithClients = optionsBuilder
                .build();

        // fail
        assertThatThrownBy(() -> parallelConsumer = initPollingAsyncConsumer(optionsWithClients))
                .as("Should error on missing group id")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Consumer", "GroupId");

        // add missing group id, now auto commit should fail
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "dummy-group");
        optionsBuilder.consumer(new KafkaConsumer<>(properties, deserializer, deserializer));
        assertThat(catchThrowable(() -> parallelConsumer = initPollingAsyncConsumer(optionsBuilder.build())))
                .as("Should error on auto commit enabled by default")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("auto", "commit", "disabled");

        // fail auto commit disabled
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        optionsBuilder.consumer(new KafkaConsumer<>(properties, deserializer, deserializer));
        assertThat(catchThrowable(() -> parallelConsumer = initPollingAsyncConsumer(optionsBuilder.build())))
                .as("Should error on auto commit enabled")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("auto", "commit", "disabled");

        // set missing auto commit
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        optionsBuilder.consumer(new KafkaConsumer<>(properties, deserializer, deserializer));
        assertThatNoException().isThrownBy(() -> parallelConsumer = initPollingAsyncConsumer(optionsBuilder.build()));
    }


    @Test
    void cantUseProduceFlowWithWrongOptions() throws InterruptedException {
        setupClients();

        // forget to supply producer
        var optionsWithClients = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerSpy)
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .build();

        setupParallelConsumerInstance(optionsWithClients);

        subscribeParallelConsumerAndMockConsumerTo(INPUT_TOPIC);

        setupData();

        var parallel = initPollingAsyncConsumer(optionsWithClients);

        assertThatThrownBy(() -> parallel.pollAndProduce((record) ->
                new ProducerRecord<>(INPUT_TOPIC, "hi there")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Producer", "options");
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    void produceMessageFlow(CommitMode commitMode) {
        setupParallelConsumerInstance(commitMode);

        parallelConsumer.pollAndProduce((ignore) -> new ProducerRecord<>("Hello", "there"));

        // let it process
        waitForSomeLoopCycles(2);

        parallelConsumer.requestCommitAsap();

        //
        await().untilAsserted(() ->
                assertCommits(of(1)));

        parallelConsumer.closeDrainFirst();


        assertThat(producerSpy.history()).hasSize(1);
    }

}

