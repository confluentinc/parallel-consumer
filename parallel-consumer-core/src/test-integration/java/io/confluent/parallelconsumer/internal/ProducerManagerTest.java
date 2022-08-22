package io.confluent.parallelconsumer.internal;

import com.google.common.truth.Truth;
import io.confluent.csid.utils.BlockedThreadAsserter;
import io.confluent.csid.utils.LatchTestUtils;
import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.integrationTests.utils.RecordFactory;
import io.confluent.parallelconsumer.state.ModelUtils;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import static java.time.Duration.ZERO;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * todo docs
 *
 * @author Antony Stubbs
 * @see ProducerManager
 */
@Tag("transactions")
@Tag("#355")
@Timeout(60)
@Slf4j
class ProducerManagerTest { //extends BrokerIntegrationTest<String, String> {

    ParallelConsumerOptions<String, String> opts = ParallelConsumerOptions.<String, String>builder()
            .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
            .build();

    PCModuleTestEnv module = new PCModuleTestEnv(opts) {
        @Override
        protected ParallelEoSStreamProcessor<String, String> pc() {
            if (parallelEoSStreamProcessor == null) {
                ParallelEoSStreamProcessor<String, String> raw = super.pc();
                parallelEoSStreamProcessor = spy(raw);

                // todo use mockito instead
//                doNothing().when(parallelEoSStreamProcessor).close(any(),any());
//                doReturn(true).when(parallelEoSStreamProcessor).isShouldCommitNow();

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

//    KafkaProducer<String, String> producer = getKcu().getProducer();
//
//    KafkaConsumer<String, String> consumer = getKcu().getConsumer();
//    ConsumerManager<String, String> cm = new ConsumerManager<>(consumer);
//
//    WorkManager<String, String> wm = new WorkManager<>(opts, consumer);

    //    ProducerManager<String, String> pm = new ProducerManager<>(producer, cm, wm, opts);
    ProducerManager<String, String> pm;
    private final ModelUtils mu = new ModelUtils(module);


    {
//        ProducerWrap mock = mock(ProducerWrap.class);
//        Mockito.when(mock.isConfiguredForTransactions()).thenReturn(true);
//        pm = new ProducerManager<>(mock, mock(ConsumerManager.class), mock(WorkManager.class), opts);
        pm = module.producerManager();
    }

    RecordFactory rf = new RecordFactory();

    /**
     * Cannot send a record during a tx commit
     */
    @Test
    void sendingGetsLockedInTx() {
        assertThat(pm).isNotTransactionCommittingInProgress();

        // should send fine, futures should finish
        var produceReadLock = pm.beginProducing();
        produceOneRecord();

        {
            // todo extract generic Blocked method tester
            AtomicBoolean blockedCommitterReturned = new AtomicBoolean(false);
            Thread blocked = new Thread(() -> {
                pm.preAcquireWork();
                pm.postCommit();
                blockedCommitterReturned.set(true);
            });
            blocked.start();

            await("pretend to start to commit - acquire commit lock")
                    .pollDelay(ofSeconds(1))
                    .untilAsserted(
                            () -> Truth.assertWithMessage("Thread should be sleeping/blocked and not have returned")
                                    .that(blockedCommitterReturned.get())
                                    .isFalse());
        }

        // pretend to finish producing records
        pm.finishProducing(produceReadLock);

        // start actual commit - acquire commit lock
        pm.preAcquireWork();

        //
        assertThat(pm).isTransactionCommittingInProgress();

        // try to send more records, which will block as tx in process
        final AtomicBoolean blockedRecordSenderReturned = new AtomicBoolean(false);
        {
            Thread blocked = new Thread(() -> {
                var produceLock = pm.beginProducing();
                pm.finishProducing(produceLock);
                blockedRecordSenderReturned.set(true);
            });
            blocked.start();

            await("starting sending records blocked")
                    .pollDelay(ofSeconds(1))
                    .untilAsserted(
                            () -> Truth.assertWithMessage("Thread should be sleeping/blocked and not have returned")
                                    .that(blockedRecordSenderReturned.get())
                                    .isFalse());

//            var blockedSends = produceOneRecord();
//            // assert these sends are blocked from sending their output record, as tx in progress
//            assertThat(blockedSends).hasSize(-1);
        }

        // pretend to finish tx
        pm.postCommit();

        //
        assertThat(pm).isNotTransactionCommittingInProgress();

        //
        await("blocked sends should only now complete").untilTrue(blockedRecordSenderReturned);
    }

    private List<ParallelConsumer.Tuple<ProducerRecord<String, String>, Future<RecordMetadata>>> produceOneRecord() {
        return pm.produceMessages(makeRecord());
    }

    private List<ProducerRecord<String, String>> makeRecord() {
        return rf.createRecords("topic", 1);
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

    /**
     * There's GOT to be a better way to test this corner case than this
     */
    @Test
    void producedRecordsCantBeInTransactionWithoutItsOffset() {

        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();


//        ProducerManager<String, String> pm = mock(ProducerManager.class);
//        var options = ParallelConsumerOptions.<String, String>builder()
//                .diModule(of(module))
//                .build();

        try (var pc = module.pc()) {
//        AbstractParallelEoSStreamProcessor<String, String> pc = mock(AbstractParallelEoSStreamProcessor.class);

            // send a record
            pc.subscribe(UniLists.of("topic"));
            pc.onPartitionsAssigned(mu.getPartitions());

            doWork(pc);

            // process inbox
            pc.processWorkCompleteMailBox(ZERO);

            // send another record, return the work, but don't process inbox
            var freshWork = mu.createFreshWork();
            pc.registerWork(freshWork);
            pc.processWorkCompleteMailBox(ZERO);

            // get the work
            List<WorkContainer<String, String>> workIfAvailable = pc.getWm().getWorkIfAvailable(100);
            assertThat(workIfAvailable).isNotEmpty();

            // being producing the record from that work
            var producingLock = pm.beginProducing();


//            pm.finishProducing(producingLock); // remove the too early produce unlock

            // this is the phase between finishing the record but not putting it into the offsets to be collected could be an issue

            // simulate the commit happening right after read lock release, but before the controller could do an inbox process
            // should block because the commit lock can't be granted
//            Assertions.assertThatThrownBy(() -> pc.commitOffsetsThatAreReady()).hasMessageContaining("Timeout");
            var commitBlocks = new BlockedThreadAsserter();
            commitBlocks.assertFunctionBlocks(pc::commitOffsetsThatAreReady, ofSeconds(2));


            // record doesn't try to send until after commit tries to start (and it should be blocked)
            pm.produceMessages(UniLists.of(mu.createProducerRecords()));


            // finish second record
            workIfAvailable.forEach(stringStringWorkContainer -> {
                pc.onUserFunctionSuccess(stringStringWorkContainer, UniLists.of());
                pc.addToMailBoxOnUserFunctionSuccess(Mockito.mock(PollContextInternal.class), stringStringWorkContainer, UniLists.of());
            });

            // finish completely the work, should free up the commit lock to be obtained, work scanned and processed (picking up this result), and the whole thing is committed
            pm.finishProducing(producingLock); // remove lock now after returning work

//            pc.processWorkCompleteMailBox(ZERO); // process the work results


            //
            await().untilAsserted(() -> Truth.assertWithMessage("commit should now have unlocked and returned")
                    .that(commitBlocks.functionHasCompleted())
                    .isTrue());


            // capturing error scenario
            final int nextExpectedOffset = 2; // as only first of two work completed
//            {
//                // error - 2 records in tx but only one of their offsets committed
//                var producer = module.producerWrap();
//                Mockito.verify(producer, times(1))
//                        .sendOffsetsToTransaction(UniMaps.of(mu.getPartition(), new OffsetAndMetadata(nextExpectedOffset, "")), mu.consumerGroupMeta());
//
//                // error - 2 times send() called - 2 records sent (should only be one, as second record send should be blocked)
//                Mockito.verify(producer, times(2))
//                        .send(any(), any());
//            }

            // correct behaviour should be
            {
                var producer = module.producerWrap();
                Mockito.verify(producer, description("Both offsets"))
                        .sendOffsetsToTransaction(UniMaps.of(mu.getPartition(), new OffsetAndMetadata(nextExpectedOffset, "")), mu.consumerGroupMeta());

//                Mockito.verify(producer, description("called once to send only one record (should only be one, as second record send should be blocked)"))
                Mockito.verify(producer, times(2)
                                .description("Should send twice, as it blocks the commit lock until it finishes, so offsets get taken only after"))
                        .send(any(), any());

            }
        }
    }

    private void doWork(ParallelEoSStreamProcessor<String, String> pc) {
        // create
        EpochAndRecordsMap<String, String> freshWork = mu.createFreshWork();
        pc.registerWork(freshWork);
        pc.processWorkCompleteMailBox(ZERO);

        // result
        List<WorkContainer<String, String>> workIfAvailable = pc.getWm().getWorkIfAvailable(100);
        assertThat(workIfAvailable).hasSize(1);

        // force record send
        var producingLock = pm.beginProducing();
        pm.produceMessages(UniLists.of(mu.createProducerRecords()));
        pm.finishProducing(producingLock);

        // result
        workIfAvailable.forEach(stringStringWorkContainer -> {
            pc.onUserFunctionSuccess(stringStringWorkContainer, UniLists.of());
            pc.addToMailBoxOnUserFunctionSuccess(Mockito.mock(PollContextInternal.class), stringStringWorkContainer, UniLists.of());
        });

        // process into wm
        pc.processWorkCompleteMailBox(ZERO);
    }


    /**
     * There's GOT to be a better way to test this corner case than this
     */
    @SneakyThrows
    @Test
    void producedRecordsCantBeInTransactionWithoutItsOffsetDirect() {

        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();


//        ProducerManager<String, String> pm = mock(ProducerManager.class);
//        var options = ParallelConsumerOptions.<String, String>builder()
//                .diModule(of(module))
//                .build();

        try (var pc = module.pc()) {
//        AbstractParallelEoSStreamProcessor<String, String> pc = mock(AbstractParallelEoSStreamProcessor.class);

            // send a record
            pc.subscribe(UniLists.of(mu.getTopic()));
            pc.onPartitionsAssigned(mu.getPartitions());
            pc.setState(State.running);

            EpochAndRecordsMap<String, String> freshWork = mu.createFreshWork();
            pc.registerWork(freshWork);

            Truth.assertThat(pm.getProducerTransactionLock().isWriteLocked()).isFalse();

            var producingLockRef = new AtomicReference<ProducerManager.ProducingLock>();
            var offset1Mutex = new CountDownLatch(1);
            Function<PollContextInternal<String, String>, List<Object>> userFunc = context -> {
                var newValue = pm.beginProducing();
                try {
                    producingLockRef.set(
                            newValue
                    );
                    log.info(context.toString());
                    if (context.offset() == 1) {
                        log.error("Blocking on {}", 1);
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
            pc.processWorkCompleteMailBox(ZERO);

            Truth.assertThat(pm.getProducerTransactionLock().isWriteLocked()).isFalse();

            // being producing the record from that work
            var producingBlocks = new BlockedThreadAsserter();

//            producingBlocks.assertFunctionBlocks(() -> {
//                Truth.assertThat(pm.getProducerTransactionLock().isWriteLocked()).isFalse();
//                producingLockRef.set(
//                        pm.beginProducing()
//                );
//
//            }, ofSeconds(2));

            // waits for user function to run and take the produce lock
//            await().untilAsserted(() -> Truth.assertThat(module.producerManager().getProducerTransactionLock().getReadLockCount()).isAtLeast(1));

            // blocks as offset 1 is blocked sending, then unblock 1, and make sure that makes us return
            var commitBlocks = new BlockedThreadAsserter();
            commitBlocks.assertUnblocksAfter(() -> {
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


//            Assertions.assertTimeoutPreemptively(ofSeconds(10), () -> pc.controlLoop(userFunc, o -> {
//            }));

            //
//            {
//                var controlMethod = new BlockedThreadAsserter();
//                controlMethod.assertFunctionBlocks(() -> {
//                    try {
//                        pc.controlLoop(userFunc, o -> {
//                        });
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                }, ofSeconds(2));
//            }
//            pc.controlLoop(stringStringPollContextInternal -> null, o -> {
//            });

            // finish producing
//            pm.finishProducing(ptod);

            // get the work
//            List<WorkContainer<String, String>> workIfAvailable = pc.getWm().getWorkIfAvailable(100);
//            assertThat(workIfAvailable).isNotEmpty();


//            new Thread(() -> {
//                producingLockRef.set(
//                        pm.beginProducing()
//                );
//            }).start();
//            await().untilAtomic(producingLockRef, Matchers.notNullValue());
//            var producingLock = producingLockRef.get();


            // record doesn't try to send until after commit tries to start (and it should be blocked)
//            pm.produceMessages(UniLists.of(mu.createProducerRecords()));


            // finish second record
//            workIfAvailable.forEach(stringStringWorkContainer -> {
//                pc.onUserFunctionSuccess(stringStringWorkContainer, UniLists.of());
//                pc.addToMailBoxOnUserFunctionSuccess(Mockito.mock(PollContextInternal.class), stringStringWorkContainer, UniLists.of());
//            });

            // finish completely the work, should free up the commit lock to be obtained, work scanned and processed (picking up this result), and the whole thing is committed
//            pm.finishProducing(producingLockRef.get()); // remove lock now after returning work

//            pc.processWorkCompleteMailBox(ZERO); // process the work results


            //
            await().untilAsserted(() -> Truth.assertWithMessage("commit should now have unlocked and returned")
                    .that(commitBlocks.functionHasCompleted())
                    .isTrue());


            // capturing error scenario
            final int nextExpectedOffset = 2; // as only first of two work completed
//            {
//                // error - 2 records in tx but only one of their offsets committed
//                var producer = module.producerWrap();
//                Mockito.verify(producer, times(1))
//                        .sendOffsetsToTransaction(UniMaps.of(mu.getPartition(), new OffsetAndMetadata(nextExpectedOffset, "")), mu.consumerGroupMeta());
//
//                // error - 2 times send() called - 2 records sent (should only be one, as second record send should be blocked)
//                Mockito.verify(producer, times(2))
//                        .send(any(), any());
//            }

            // correct behaviour should be
            {
                var producer = module.producerWrap();
                Mockito.verify(producer, description("Both offsets are represented in base commit"))
                        .sendOffsetsToTransaction(UniMaps.of(mu.getPartition(), new OffsetAndMetadata(nextExpectedOffset, "")), mu.consumerGroupMeta());

//                Mockito.verify(producer, description("called once to send only one record (should only be one, as second record send should be blocked)"))
                Mockito.verify(producer, times(2)
                                .description("Should send twice, as it blocks the commit lock until it finishes, so offsets get taken only after"))
                        .send(any(), any());

            }
        }
    }

    // todo test allowEagerProcessingDuringTransactionCommit

}