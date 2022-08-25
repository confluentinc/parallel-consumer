package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.BlockedThreadAsserter;
import io.confluent.csid.utils.LatchTestUtils;
import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.ModelUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ManagedTruth.assertWithMessage;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.internal.ProducerManager.ProducerState.*;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Covers transaction state systems, and their blocking behaiviour towards sending records and the reverse.
 *
 * @author Antony Stubbs
 * @see ProducerManager
 */
@Tag("transactions")
@Tag("#355")
@Timeout(60)
@Slf4j
class ProducerManagerTest {

    ParallelConsumerOptions<String, String> opts = ParallelConsumerOptions.<String, String>builder()
            .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
            .build();

    PCModuleTestEnv module = new PCModuleTestEnv(opts) {
        @Override
        protected AbstractParallelEoSStreamProcessor<String, String> pc() {
            if (parallelEoSStreamProcessor == null) {
                AbstractParallelEoSStreamProcessor<String, String> raw = super.pc();
                parallelEoSStreamProcessor = spy(raw);

                parallelEoSStreamProcessor = new ParallelEoSStreamProcessor<>(options(), this) {
                    @Override
                    protected boolean isTimeToCommitNow() {
                        return true;
                    }

                    @Override
                    public void close(final Duration timeout, final DrainingMode drainMode) {
                    }
                };
            }
            return parallelEoSStreamProcessor;
        }
    };

    private final ModelUtils mu = new ModelUtils(module);

    ProducerManager<String, String> pm = module.producerManager();


    /**
     * Cannot send a record during a tx commit
     */
    @Test
    void sendingGetsLockedInTx() {
        assertThat(pm).isNotTransactionCommittingInProgress();

        // should send fine, futures should finish
        var produceReadLock = pm.beginProducing();
        produceOneRecord();

        new BlockedThreadAsserter().assertFunctionBlocks(() -> {
            // commit sequence
            pm.preAcquireWork();
            pm.postCommit();
        });

        // pretend to finish producing records, give the lock back
        log.debug("Unlocking...");
        pm.finishProducing(produceReadLock);


        // start actual commit - acquire commit lock
        pm.preAcquireWork();

        //
        assertThat(pm).isTransactionCommittingInProgress();

        // try to send more records, which will block as tx in process
        // Thread should be sleeping/blocked and not have returned
        var blockedRecordSenderReturned = new BlockedThreadAsserter();
        blockedRecordSenderReturned.assertFunctionBlocks(() -> {
            log.debug("Starting sending records - will block due to open commit");
            var produceLock = pm.beginProducing();
            log.debug("Then after released by finishing tx, complete the producing");
            pm.finishProducing(produceLock);
        });


        // pretend to finish tx
        pm.postCommit();

        //
        assertThat(pm).isNotTransactionCommittingInProgress();

        //
        await("blocked sends should only now complete").until(blockedRecordSenderReturned::functionHasCompleted);
    }

    private List<ParallelConsumer.Tuple<ProducerRecord<String, String>, Future<RecordMetadata>>> produceOneRecord() {
        return pm.produceMessages(makeRecord());
    }

    private List<ProducerRecord<String, String>> makeRecord() {
        return mu.createProducerRecords("topic", 1);
    }

    /**
     * Make sure transaction get started lazy - only when a record is sent, not proactively
     */
    @Test
    void txOnlyStartedUponMessageSend() {
        assertThat(pm).isNotTransactionCommittingInProgress();
        assertThat(pm).stateIs(INIT);

        assertWithMessage("Transaction is started as not open")
                .that(pm)
                .transactionNotOpen();

        {
            var produceLock = pm.beginProducing();

            {
                var notBlockedSends = produceOneRecord();
            }

            assertThat(pm).stateIs(BEGIN);
            assertThat(pm).transactionOpen();

            {
                var notBlockedSends = produceOneRecord();
            }

            pm.finishProducing(produceLock);
        }

        pm.preAcquireWork();

        assertThat(pm).isTransactionCommittingInProgress();

        pm.commitOffsets(UniMaps.of(), new ConsumerGroupMetadata(""));

        assertThat(pm).isTransactionCommittingInProgress();

        pm.postCommit();

        assertThat(pm).isNotTransactionCommittingInProgress();

        //
        assertWithMessage("A new transaction hasn't been opened")
                .that(pm)
                .transactionNotOpen();

        // do another round of producing and check state
        {
            var producingLock = pm.beginProducing();
            assertThat(pm).transactionNotOpen();
            produceOneRecord();
            assertThat(pm).transactionOpen();
            pm.finishProducing(producingLock);
            assertThat(pm).transactionOpen();
            pm.preAcquireWork();
            assertThat(pm).transactionOpen();
            pm.commitOffsets(UniMaps.of(), new ConsumerGroupMetadata(""));
            assertThat(pm).transactionNotOpen();
            assertThat(pm).stateIs(COMMIT);
        }
    }

    @SneakyThrows
    @Test
    void producedRecordsCantBeInTransactionWithoutItsOffsetDirect() {

        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        try (var pc = module.pc()) {

            // send a record
            pc.subscribe(UniLists.of(mu.getTopic()));
            pc.onPartitionsAssigned(mu.getPartitions());
            pc.setState(State.running);

            EpochAndRecordsMap<String, String> freshWork = mu.createFreshWork();
            pc.registerWork(freshWork);

            Truth.assertThat(pm.getProducerTransactionLock().isWriteLocked()).isFalse();

            var producingLockRef = new AtomicReference<ProducerManager.ProducingLock>();
            var offset1Mutex = new CountDownLatch(1);
            var blockedOn1 = new AtomicBoolean(false);
            Function<PollContextInternal<String, String>, List<Object>> userFunc = context -> {
                var newValue = pm.beginProducing();
                try {
                    producingLockRef.set(
                            newValue
                    );
                    log.info(context.toString());
                    if (context.offset() == 1) {
                        log.debug("Blocking on {}", 1);
                        blockedOn1.set(true);
                        LatchTestUtils.awaitLatch(offset1Mutex);
                    }

                    // use real user function wrap
                    module.producerWrap().send(mock(ProducerRecord.class), (a, b) -> {
                    });
                    return UniLists.of();
                } finally {
                    // this unlocks the produce lock too early - should be after WC returned. Need a call back? plugin? Should refactor the wrapped user function to can construct it?
                    // also without using wrapped user function- we're not testing something important
                    newValue.unlock();
                }
            };


            Truth.assertThat(pm.getProducerTransactionLock().isWriteLocked()).isFalse();


            // won't block because offset 0 goes through
            pc.controlLoop(userFunc, o -> {
            });


            Truth.assertThat(pm.getProducerTransactionLock().isWriteLocked()).isFalse();


            // won't block - not dirty
            pc.controlLoop(userFunc, o -> {
            });

            // send another record, return the work, but don't process inbox
            freshWork = mu.createFreshWork();
            pc.registerWork(freshWork);
            // will first try to commit - fine no produce lock yet
            // then gets the work, distributes it
            // will then return
            // -- in the worker thread - will trigger the block and hold the produce lock
            pc.controlLoop(userFunc, o -> {
            });

            Truth.assertThat(pm.getProducerTransactionLock().isWriteLocked()).isFalse();

            // blocks as offset 1 is blocked sending and so cannot acquire commit lock
            // unblock 1 as unblocking function, and make sure that makes us return
            var msg = "Ensure expected produce lock is now held by blocked worker thread";
            log.debug(msg);
            await(msg).untilAtomic(blockedOn1, Matchers.is(Matchers.equalTo(true)));
            var commitBlocks = new BlockedThreadAsserter();
            commitBlocks.assertUnblocksAfter(() -> {
                log.debug("Running control loop which should block until offset 1 is released by finishing produce");
                try {
                    pc.controlLoop(userFunc, o -> {
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, () -> {
                log.debug("Unblocking offset processing offset1Mutex...");
                offset1Mutex.countDown();
            }, ofSeconds(2));

            //
            await().untilAsserted(() -> Truth.assertWithMessage("commit should now have unlocked and returned")
                    .that(commitBlocks.functionHasCompleted())
                    .isTrue());


            final int nextExpectedOffset = 2; // as only first of two work completed
            {
                var producer = module.producerWrap();
                Mockito.verify(producer, description("Both offsets are represented in base commit"))
                        .sendOffsetsToTransaction(UniMaps.of(mu.getPartition(), new OffsetAndMetadata(nextExpectedOffset, "")), mu.consumerGroupMeta());

                Mockito.verify(producer, times(2)
                                .description("Should send twice, as it blocks the commit lock until it finishes, so offsets get taken only after"))
                        .send(any(), any());

            }
        }
    }

    // todo test allowEagerProcessingDuringTransactionCommit

    @Test
    @Disabled
        // todo implement or delete
    void commitLockTimeoutShouldRecover() {
    }

    @Test
    @Disabled
        // todo implement or delete
    void produceLockTimeoutShouldRecover() {
    }


    /**
     * Test aborting the second tx has only first plus nothing in result topic
     */
    @Test
    // todo implement or delete
    @Disabled
    void abortedSecondTransaction() {
        Truth.assertThat(true).isFalse();
    }


    /**
     * Test aborting the first tx ends up with nothing
     */
    @Test
    // todo implement or delete
    @Disabled
    void abortedBothTransactions() {
        // do the above again, but instead abort the transaction
        // assert nothing on result topic
        Truth.assertThat(true).isFalse();
    }

}