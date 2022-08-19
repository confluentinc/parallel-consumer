package io.confluent.parallelconsumer.internal;

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.integrationTests.utils.RecordFactory;
import io.confluent.parallelconsumer.state.ModelUtils;
import io.confluent.parallelconsumer.state.WorkContainer;
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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ManagedTruth.assertWithMessage;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.internal.ProducerManager.ProducerState.*;
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
class ProducerManagerTest { //extends BrokerIntegrationTest<String, String> {

    ParallelConsumerOptions<String, String> opts = ParallelConsumerOptions.<String, String>builder()
            .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
            .build();

    PCModuleTestEnv module = new PCModuleTestEnv(opts);

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
                    .pollDelay(Duration.ofSeconds(1))
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
                    .pollDelay(Duration.ofSeconds(1))
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
            pc.processWorkCompleteMailBox(Duration.ZERO);

            // send another record, return the work, but don't process inbox
            var freshWork = mu.createFreshWork();
            pc.registerWork(freshWork);
            var producingLock = pm.beginProducing();
            pm.produceMessages(UniLists.of(mu.createProducerRecords()));
            pm.finishProducing(producingLock);

            // this is the phase between finishing the record but not putting it into the offsets to be collected could be an issue

            // simulate the commit happening right after read lock release, but before the controller could do an inbox process
            pc.commitOffsetsThatAreReady();

            // capturing error scenario

            final int nextExpectedOffset = 1; // as only first of two work completed
            {
                // error - 2 records in tx but only one of their offsets committed
                var producer = module.producerWrap();
                Mockito.verify(producer, atMostOnce())
                        .sendOffsetsToTransaction(UniMaps.of(mu.getPartition(), new OffsetAndMetadata(nextExpectedOffset, "")), mu.consumerGroupMeta());

                // error - 2 times send() called - 2 records sent (should only be one, as second record send should be blocked)
                Mockito.verify(producer, times(2))
                        .send(any(), any());
            }

            // correct behaviour should be
            {
                var producer = module.producerWrap();
                Mockito.verify(producer, description("Only single record in tx and the first offset only"))
                        .sendOffsetsToTransaction(UniMaps.of(mu.getPartition(), new OffsetAndMetadata(nextExpectedOffset, "")), mu.consumerGroupMeta());

                Mockito.verify(producer, description("called once to send only one record (should only be one, as second record send should be blocked)"))
                        .send(any(), any());

            }
        }
    }

    private void doWork(ParallelEoSStreamProcessor<String, String> pc) {
        // create
        EpochAndRecordsMap<String, String> freshWork = mu.createFreshWork();
        pc.registerWork(freshWork);
        pc.processWorkCompleteMailBox(Duration.ZERO);

        // result
        List<WorkContainer<String, String>> workIfAvailable = pc.getWm().getWorkIfAvailable(100);
        assertThat(workIfAvailable).isNotEmpty();

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
        pc.processWorkCompleteMailBox(Duration.ZERO);
    }

}