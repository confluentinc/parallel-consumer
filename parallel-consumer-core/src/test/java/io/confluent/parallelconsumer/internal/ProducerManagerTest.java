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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ManagedTruth.assertWithMessage;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.internal.ProducerWrapper.ProducerState.*;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Covers transaction state systems, and their blocking behaiviour towards sending records and the reverse.
 *
 * @author Antony Stubbs
 * @see ProducerManager
 * @see io.confluent.parallelconsumer.integrationTests.TransactionTimeoutsTest for integration tests checking timeout
 *         behaiviour
 */
@Tag("transactions")
@Tag("#355")
@Timeout(60)
@Slf4j
class ProducerManagerTest {

    ParallelConsumerOptions<String, String> opts;

    PCModuleTestEnv module;

    ModelUtils mu;

    ProducerManager<String, String> producerManager;

    /**
     * Default settings
     */
    @BeforeEach
    void setup() {
        setup(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitLockAcquisitionTimeout(ofSeconds(2)));
    }

    private void setup(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder) {
        opts = optionsBuilder.build();

        buildModule(opts);

        module = buildModule(opts);

        mu = new ModelUtils(module);

        producerManager = module.producerManager();
    }

    private PCModuleTestEnv buildModule(ParallelConsumerOptions<String, String> opts) {
        return new PCModuleTestEnv(opts) {
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
    }


    /**
     * Cannot send a record during a tx commit
     */
    @SneakyThrows
    @Test
    void sendingGetsLockedInTx() {
        assertThat(producerManager).isNotTransactionCommittingInProgress();

        // should send fine, futures should finish
        var produceReadLock = producerManager.beginProducing(mock(PollContextInternal.class));
        produceOneRecord();

        // acquire work should block
        var blockedCommit = new BlockedThreadAsserter();
        blockedCommit.assertFunctionBlocks(() -> {
            // commit sequence
            try {
                producerManager.preAcquireOffsetsToCommit();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // releases the commit lock that was acquired
            producerManager.postCommit();
        });

        // pretend to finish producing records, give the lock back
        log.debug("Unlocking produce lock...");
        producerManager.finishProducing(produceReadLock); // triggers commit lock to become acquired as the produce lock is now released

        log.debug("Waiting for commit lock to release...");
        blockedCommit.awaitReturnFully();

        // start actual commit - acquire commit lock
        producerManager.preAcquireOffsetsToCommit();

        //
        assertThat(producerManager).isTransactionCommittingInProgress();

        // try to send more records, which will block as tx in process
        // Thread should be sleeping/blocked and not have returned
        var blockedRecordSenderReturned = new BlockedThreadAsserter();
        blockedRecordSenderReturned.assertFunctionBlocks(() -> {
            log.debug("Starting sending records - will block due to open commit");
            ProducerManager<String, String>.ProducingLock produceLock = null;
            try {
                produceLock = producerManager.beginProducing(mock(PollContextInternal.class));
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
            log.debug("Then after released by finishing tx, complete the producing");
            producerManager.finishProducing(produceLock);
        });


        // pretend to finish tx
        producerManager.postCommit();

        //
        assertThat(producerManager).isNotTransactionCommittingInProgress();

        //
        await("blocked sends should only now complete").until(blockedRecordSenderReturned::functionHasCompleted);
    }

    private List<ParallelConsumer.Tuple<ProducerRecord<String, String>, Future<RecordMetadata>>> produceOneRecord() {
        return producerManager.produceMessages(makeRecord());
    }

    private List<ProducerRecord<String, String>> makeRecord() {
        return mu.createProducerRecords("topic", 1);
    }

    /**
     * Make sure transaction get started lazy - only when a record is sent, not proactively
     */
    @SneakyThrows
    @Test
    void txOnlyStartedUponMessageSend() {
        assertThat(producerManager).isNotTransactionCommittingInProgress();
        assertThat(producerManager).stateIs(INIT);

        assertWithMessage("Transaction is started as not open")
                .that(producerManager)
                .transactionNotOpen();

        {
            var produceLock = producerManager.beginProducing(mock(PollContextInternal.class));

            {
                var notBlockedSends = produceOneRecord();
            }

            assertThat(producerManager).stateIs(BEGIN);
            assertThat(producerManager).transactionOpen();

            {
                var notBlockedSends = produceOneRecord();
            }

            producerManager.finishProducing(produceLock);
        }

        producerManager.preAcquireOffsetsToCommit();

        assertThat(producerManager).isTransactionCommittingInProgress();

        producerManager.commitOffsets(UniMaps.of(), new ConsumerGroupMetadata(""));

        assertThat(producerManager).isTransactionCommittingInProgress();

        producerManager.postCommit();

        assertThat(producerManager).isNotTransactionCommittingInProgress();

        //
        assertWithMessage("A new transaction hasn't been opened")
                .that(producerManager)
                .transactionNotOpen();

        // do another round of producing and check state
        {
            var producingLock = producerManager.beginProducing(mock(PollContextInternal.class));
            assertThat(producerManager).transactionNotOpen();
            produceOneRecord();
            assertThat(producerManager).transactionOpen();
            producerManager.finishProducing(producingLock);
            assertThat(producerManager).transactionOpen();
            producerManager.preAcquireOffsetsToCommit();
            assertThat(producerManager).transactionOpen();
            producerManager.commitOffsets(UniMaps.of(), new ConsumerGroupMetadata(""));
            assertThat(producerManager).transactionNotOpen();
            assertThat(producerManager).stateIs(COMMIT);
        }
    }

    @SneakyThrows
    @Test
    void producedRecordsCantBeInTransactionWithoutItsOffsetDirect() {
        // custom settings
        setup(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER));

        try (var pc = module.pc()) {
            pc.subscribe(UniLists.of(mu.getTopic()));
            pc.onPartitionsAssigned(mu.getPartitions());
            pc.setState(State.running);

            // "send" one record
            EpochAndRecordsMap<String, String> freshWork = mu.createFreshWork();
            pc.registerWork(freshWork);

            assertThat(producerManager).getProducerTransactionLock().isNotWriteLocked();


            var producingLockRef = new AtomicReference<ProducerManager.ProducingLock>();
            var offset1Mutex = new CountDownLatch(1);
            var blockedOn1 = new AtomicBoolean(false);
            // todo refactor to use real user function directly
            Function<PollContextInternal<String, String>, List<Object>> userFunc = context -> {
                ProducerManager<String, String>.ProducingLock newValue = null;
                try {
                    newValue = producerManager.beginProducing(mock(PollContextInternal.class));
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
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


            assertThat(producerManager).getProducerTransactionLock().isNotWriteLocked();


            // won't block because offset 0 goes through
            // distributes first work
            pc.controlLoop(userFunc, o -> {
            });


            // change to TM?
            assertThat(producerManager).getProducerTransactionLock().isNotWriteLocked();

            //
            {
                var msg = "wait for first record to finish";
                log.debug(msg);
                await(msg).untilAsserted(() -> assertThat(pc.getWorkMailBox()).hasSize(1));
            }

            // send another record, register the work
            freshWork = mu.createFreshWork();
            pc.registerWork(freshWork);

            // will first try to commit - which will work fine, as there's no produce lock isn't held yet (off 0 goes through fine)
            // then it will get the work, distributes it
            // will then return
            // -- in the worker thread - will trigger the block and hold the produce lock
            pc.controlLoop(userFunc, o -> {
            });

            //
            assertThat(producerManager).getProducerTransactionLock().isNotWriteLocked();

            // blocks, as offset 1 is blocked sending and so cannot acquire commit lock
            var msg = "Ensure expected produce lock is now held by blocked worker thread";
            log.debug(msg);
            await(msg).untilTrue(blockedOn1);


            var commitBlocks = new BlockedThreadAsserter();
            // unblock 1 as unblocking function, and make sure that makes us return
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
            }, ofSeconds(10));

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

    @Test
    void testOptions() {
        assertThrows(IllegalArgumentException.class, () ->
                ParallelConsumerOptions.builder()
                        .consumer(mock(Consumer.class))
                        .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                        .build()
                        .validate());


        assertThrows(IllegalArgumentException.class, () ->
                ParallelConsumerOptions.builder()
                        .consumer(mock(Consumer.class))
                        .allowEagerProcessingDuringTransactionCommit(true)
                        .build()
                        .validate());
    }

}