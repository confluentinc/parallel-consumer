package io.confluent.parallelconsumer.internal;

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.integrationTests.utils.RecordFactory;
import io.confluent.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ManagedTruth.assertWithMessage;
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

    ParallelConsumerOptions<String, String> opts = ParallelConsumerOptions.<String, String>builder().build();

//    KafkaProducer<String, String> producer = getKcu().getProducer();
//
//    KafkaConsumer<String, String> consumer = getKcu().getConsumer();
//    ConsumerManager<String, String> cm = new ConsumerManager<>(consumer);
//
//    WorkManager<String, String> wm = new WorkManager<>(opts, consumer);

    //    ProducerManager<String, String> pm = new ProducerManager<>(producer, cm, wm, opts);
    ProducerManager<String, String> pm = new ProducerManager<>(mock(Producer.class), mock(ConsumerManager.class), mock(WorkManager.class), opts);


    RecordFactory rf = new RecordFactory();

    /**
     * Cannot send a record during a tx commit
     */
    @Test
    void sendingGetsLockedInTx() {
        assertThat(pm).isNotTransactionCommittingInProgress();

        // should send fine, futures should finish
        var produceReadLock = pm.startProducing();
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
                ReentrantReadWriteLock.ReadLock produceLock = pm.startProducing();
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

        assertWithMessage("Transaction is started as not open")
                .that(pm)
                .transactionNotOpen();

        {
            var notBlockedSends = produceOneRecord();
        }

        assertThat(pm).transactionOpen();

        {
            var notBlockedSends = produceOneRecord();
        }

        pm.preAcquireWork();

        assertThat(pm).isTransactionCommittingInProgress();

        pm.commitOffsets(null, null);

        assertThat(pm).isTransactionCommittingInProgress();

        pm.postCommit();

        assertThat(pm).isNotTransactionCommittingInProgress();

        //
        assertWithMessage("A new transaction hasn't been opened")
                .that(pm)
                .transactionNotOpen();
    }

}