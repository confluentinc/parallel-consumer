package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.WallClock;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.csid.utils.GeneralTestUtils.time;
import static io.confluent.csid.utils.KafkaUtils.toTP;
import static io.confluent.csid.utils.Range.range;
import static java.time.Duration.ofMillis;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static pl.tlinkowski.unij.api.UniLists.of;

@Timeout(value = 10, unit = SECONDS)
@Slf4j
public class ParallelEoSStreamProcessorImplTest extends ParallelEoSStreamProcessorImplTestBase {

    static class MyAction implements Function<ConsumerRecord<String, String>, String> {

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

    @Test
    @SneakyThrows
    public void failingActionNothingCommitted() {
        parallelConsumer.poll((ignore) -> {
            throw new RuntimeException("My user's function error");
        });

        // let it process
        int loops = 3;
        waitForSomeLoopCycles(loops);

        //
        assertCommits(of(0), "All erroring, nothing committed except initial");
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> maps = producerSpy.consumerGroupOffsetsHistory();
        List<OffsetAndMetadata> metas = new ArrayList<>();
        for (final Map<String, Map<TopicPartition, OffsetAndMetadata>> map : maps) {
            for (final Map<TopicPartition, OffsetAndMetadata> value : map.values()) {
                for (final OffsetAndMetadata offsetAndMetadata : value.values()) {
                    metas.add(offsetAndMetadata);
                }
            }
        }
        for (final OffsetAndMetadata meta : metas) {
            assertThat(meta.offset()).isEqualTo(0L);
            TreeSet<Long> incompletes =
                    OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(0, meta.metadata()).getRight();
            assertThat(incompletes).containsExactly(0L);
        }
    }

    @Test
    @SneakyThrows
    public void offsetsAreNeverCommittedForMessagesStillInFlightSimplest() {
        sendSecondRecord(consumerSpy);

        var locks = constructLatches(2);

        var processedStates = new HashMap<Integer, Boolean>();

        var startBarrierLatch = new CountDownLatch(1);

        // finish processing only msg 1
        parallelConsumer.poll((ignore) -> {
            startBarrierLatch.countDown();
            int offset = (int) ignore.offset();
            awaitLatch(locks, offset);
            processedStates.put(offset, true);
        });

        awaitLatch(startBarrierLatch);

        assertThat(parallelConsumer.getWm().getInFlightCount()).isEqualTo(2);

        // finish processing 1
        releaseAndWait(locks, 1);

        // make sure no offsets are committed
        assertCommits(of(0), "Partition is blocked");

        // So it's data setup can be used in other tests, finish 0
        releaseAndWait(locks, 0);

        log.debug("Closing...");
        parallelConsumer.close();

        assertThat(processedStates.values()).as("sanity - all expected messages are processed").containsExactly(true, true);
    }

    @Test
    public void offsetsAreNeverCommittedForMessagesStillInFlightShort() {
        offsetsAreNeverCommittedForMessagesStillInFlightSimplest();

        log.info("Test start");

        // make sure offset 1, not 0 is committed
        // check only 1 is now committed, not committing 0 as well is a performance thing
        verify(producerSpy,
                after(verificationWaitDelay)
                        .atLeast(1)
                        .description("wait for at least one commit call"))
                .commitTransaction();

        assertCommits(of(0, 2), "Only one of the two offsets committed, as they were coalesced for efficiency");
    }

    @Disabled
    @Test
    public void offsetsAreNeverCommittedForMessagesStillInFlightLong() {
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

    @Test
    public void offsetCommitsAreIsolatedPerPartition() {
        sendSecondRecord(consumerSpy);

        // send three messages - 0,1, to one partition and 3,4 to another partition petitions
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
                // ignore
            }
        });

        // finish processing 1
        releaseAndWait(locks, 1);

        // make sure only base offsets are committed
        assertCommits(of(0, 2));
        assertCommitLists(of(of(0), of(2)));

        // finish 2
        releaseAndWait(locks, 2);
        waitForOneLoopCycle();

        // make sure only 2 on it's partition of committed
        assertCommits(of(0, 2, 3));
        assertCommitLists(of(of(0), of(2, 3)));

        // finish 0
        releaseAndWait(locks, 0);

        // make sure offset 0 and 1 is committed
        assertCommitLists(of(of(0, 2), of(2, 3)));

        // finish 3
        releaseAndWait(locks, 3);

        //
        assertCommitLists(of(of(0, 2), of(2, 3, 4)));
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

    @Test
    @SneakyThrows
    public void controlFlowException() {
        //
        WallClock mock = mock(WallClock.class);
        when(mock.getNow()).thenThrow(new RuntimeException("My fake control loop error"));
        parallelConsumer.setClock(mock);

        //
        parallelConsumer.poll((ignore) -> {
            // ignore
        });

        // close and retrieve exception in control loop
        assertThatThrownBy(() -> {
            parallelConsumer.close(false);
        }).hasMessageContainingAll("Error", "poll", "thread", "fake control");
    }

    @Test
    @SneakyThrows
    public void testVoid() {
        int expected = 1;
        var msgCompleteBarrier = new CountDownLatch(expected);
        parallelConsumer.poll((record) -> {
            waitForInitialBootstrapCommit();
            myRecordProcessingAction.apply(record);
            msgCompleteBarrier.countDown();
        });

        awaitLatch(msgCompleteBarrier);

        waitForSomeLoopCycles(1);

        assertCommits(of(0, 1));

        verify(myRecordProcessingAction, times(expected)).apply(any());
        verify(producerSpy, atLeastOnce()).commitTransaction();
        verify(producerSpy, atLeastOnce()).sendOffsetsToTransaction(anyMap(), ArgumentMatchers.<ConsumerGroupMetadata>any());
    }

    /**
     * Allow time for offset zero to be committed
     */
    private void waitForInitialBootstrapCommit() {
        await("for initial commit")
                .pollDelay(ofMillis(0))
                .pollInterval(ofMillis(DEFAULT_COMMIT_INTERVAL_MAX_MS / 2))
                .untilAsserted(() -> assertCommits(of(0)));
    }

    @Test
    public void testProducerStep() {
        ProducerRecord<String, String> outMsg = new ProducerRecord(OUTPUT_TOPIC, "");
        RecordMetadata prodResult = parallelConsumer.produceMessage(outMsg);
        assertThat(prodResult).isNotNull();

        List<ProducerRecord<String, String>> history = producerSpy.history();
        assertThat(history).hasSize(1);
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

    @SneakyThrows
    @Test
    @Disabled
    public void processInKeyOrder() {
        ParallelConsumerOptions options = ParallelConsumerOptions.builder().ordering(KEY).build();
        setupAsyncConsumerInstance(options);

        // sanity check
        assertThat(parallelConsumer.wm.getOptions().getOrdering()).isEqualTo(KEY);

        primeFirstRecord();

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

    @SneakyThrows
    @Test
    public void processInKeyOrderWorkNotReturnedDoesntBreakCommits() {
        ParallelConsumerOptions options = ParallelConsumerOptions.builder().ordering(KEY).build();
        setupAsyncConsumerInstance(options);
        primeFirstRecord();

        sendSecondRecord(consumerSpy);

        // sanity check
        assertThat(parallelConsumer.wm.getOptions().getOrdering()).isEqualTo(KEY);

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

        waitForOneLoopCycle();

        assertThat(polled).as("input data check").hasSize(3);

        //
        awaitLatch(twoLoopLatch);
        waitForOneLoopCycle();

        //
        try {  // simpler way of making the bootstrap commit optional in the results, than adding the required barrier
            // locks to ensure it's existence, which has been tested else where
            assertCommits(of(0, 1), "Only 0 should be committed, as even though 2 is also finished, 1 should be " +
                    "blocking the partition");
        } catch (AssertionError e) {
            assertCommits(of(1), "Bootstrap commit is optional. See msg in code above");
        }

        //
        msg1latch.countDown(); // release remaining processing lock

        //
        awaitLatch(fourLoopLatch); // wait for some loops

        // one more step
        waitForOneLoopCycle();

        //
        try { // see above
            assertCommits(of(0, 1, 3), "Remaining two records should be committed as a single offset");
        } catch (AssertionError e) {
            assertCommits(of(1, 3), "Bootstrap commit is optional. See msg in code above");
        }
    }

    @Test
    public void closeAfterSingleMessageShouldBeEventBasedFast() {
        var msgCompleteBarrier = new CountDownLatch(1);

        parallelConsumer.poll((ignore) -> {
            waitForInitialBootstrapCommit();
            log.info("Message processed: {} - noop", ignore.offset());
            msgCompleteBarrier.countDown();
        });

        awaitLatch(msgCompleteBarrier);

        // allow for offset to be committed
        waitForOneLoopCycle();

        assertCommits(of(0, 1));

        // close
        Duration time = time(() -> {
            parallelConsumer.close();
        });

        //
        assertThat(time).as("Should not be blocked for any reason")
                .isLessThan(ofMillis(500));
    }

    @Test
    public void closeWithoutRunningShouldBeEventBasedFast() {
        parallelConsumer.close(false);
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

}
