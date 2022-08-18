package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.RecordFactory;
import io.confluent.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Future;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ManagedTruth.assertWithMessage;

/**
 * todo docs
 *
 * @author Antony Stubbs
 * @see ProducerManager
 */
class ProducerManagerTest extends BrokerIntegrationTest<String, String> {

    ParallelConsumerOptions<String, String> opts = ParallelConsumerOptions.<String, String>builder().build();

    KafkaProducer<String, String> producer = getKcu().getProducer();

    KafkaConsumer<String, String> consumer = getKcu().getConsumer();
    ConsumerManager<String, String> cm = new ConsumerManager<>(consumer);

    WorkManager<String, String> wm = new WorkManager<>(opts, consumer);

    ProducerManager<String, String> pm = new ProducerManager<>(producer, cm, wm, opts);

    RecordFactory rf = new RecordFactory();

    /**
     * todo docs
     */
    @Test
    void sendingGetsLockedInTx() {
        assertThat(pm).isNotTransactionCommittingInProgress();

        // should send fine, futures should finish
        var notBlockedSends = produceOneRecord();
        assertThat(notBlockedSends).hasSize(-1);

        // pretend to start to commit
        pm.preAcquireWork();

        //
        assertThat(pm).isTransactionCommittingInProgress();

        // these futures should block
        var blockedSends = produceOneRecord();
        assertThat(blockedSends).hasSize(-1);

        // pretend to finish tx
        pm.postCommit();

        //
        assertThat(pm).isNotTransactionCommittingInProgress();


        // blocked to send should only now complete
        assertThat(blockedSends).hasSize(-1);
    }

    private List<ParallelConsumer.Tuple<ProducerRecord<String, String>, Future<RecordMetadata>>> produceOneRecord() {
        return pm.produceMessages(makeRecord());
    }

    private List<ProducerRecord<String, String>> makeRecord() {
        return rf.createRecords("topic", 1);
    }

    /**
     * todo docs
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