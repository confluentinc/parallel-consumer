package io.confluent.parallelconsumer.internal;

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.RecordFactory;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
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
import static org.mockito.Mockito.mock;

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

//    KafkaProducer<String, String> producer = getKcu().getProducer();
//
//    KafkaConsumer<String, String> consumer = getKcu().getConsumer();
//    ConsumerManager<String, String> cm = new ConsumerManager<>(consumer);
//
//    WorkManager<String, String> wm = new WorkManager<>(opts, consumer);

    //    ProducerManager<String, String> pm = new ProducerManager<>(producer, cm, wm, opts);
    ProducerManager<String, String> pm;

    {
        ProducerWrap mock = mock(ProducerWrap.class);
        Mockito.when(mock.isConfiguredForTransactions()).thenReturn(true);
        pm = new ProducerManager<>(mock, mock(ConsumerManager.class), mock(WorkManager.class), opts);
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
        ProducerManager<String, String> pm = mock(ProducerManager.class);
        var options = ParallelConsumerOptions.builder()
                .producer()
                .build();
        try (var pc = new ParallelEoSStreamProcessor<String, String>(options)) {
//        AbstractParallelEoSStreamProcessor<String, String> pc = mock(AbstractParallelEoSStreamProcessor.class);

            // send a record
            pc.onUserFunctionSuccess(mock(WorkContainer.class), UniLists.of());

            // process inbox
            pc.processWorkCompleteMailBox(Duration.ZERO);

            // send another record, don't process inbox
            pc.onUserFunctionSuccess(mock(WorkContainer.class), UniLists.of());

            // commit
            pc.commitOffsetsThatAreReady();

            // error - 2 records in tx but only one of their offsets committed
            Mockito.verify(pm, Mockito.atLeastOnce()).commitOffsets(UniMaps.of(), new ConsumerGroupMetadata(""));
        }
    }

}