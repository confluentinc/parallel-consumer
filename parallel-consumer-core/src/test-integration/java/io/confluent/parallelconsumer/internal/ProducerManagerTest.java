package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;
import java.util.concurrent.Future;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ManagedTruth.assertWithMessage;
import static org.mockito.Mockito.mock;

/**
 * todo docs
 *
 * @author Antony Stubbs
 * @see ProducerManager
 */
class ProducerManagerTest {

    TopicPartition tp = new TopicPartition("my-topic", 0);

    ProducerManager<Object, Object> pm;

    {
        Producer mock = mock(Producer.class);
        WorkManager wm = mock(WorkManager.class);
        ConsumerManager cm = mock(ConsumerManager.class);
        ParallelConsumerOptions<Object, Object> options = ParallelConsumerOptions.builder().build();
        pm = new ProducerManager<>(mock, cm, wm, options);
    }

    /**
     * todo docs
     */
    @Test
    void sendingGetsLockedInTx() {
        assertThat(pm).isNotTransactionCommittingInProgress();

        // should send fine, futures should finish
        var notBlockedSends = send();
        assertThat(notBlockedSends).hasSize(-1);

        // pretend to start to commit
        pm.preAcquireWork();

        //
        assertThat(pm).isTransactionCommittingInProgress();

        // these futures should block
        var blockedSends = send();
        assertThat(blockedSends).hasSize(-1);

        // pretend to finish tx
        pm.postCommit();

        //
        assertThat(pm).isNotTransactionCommittingInProgress();


        // blocked to send should only now complete
        assertThat(blockedSends).hasSize(-1);
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
            var notBlockedSends = send();
        }

        assertThat(pm).transactionOpen();

        {
            var notBlockedSends = send();
        }

        pm.preAcquireWork();

        assertThat(pm).isTransactionCommittingInProgress();

        pm.commitOffsets(UniMaps.of(tp, new OffsetAndMetadata(0)), new grop))

        assertThat(pm).isTransactionCommittingInProgress();

        pm.postCommit();

        assertThat(pm).isNotTransactionCommittingInProgress();

        //
        assertWithMessage("A new transaction hasn't been opened")
                .that(pm)
                .transactionNotOpen();
    }

    private List<ParallelConsumer.Tuple<ProducerRecord<Object, Object>, Future<RecordMetadata>>> send() {
        return pm.produceMessages(UniLists.of(new ProducerRecord<>(tp.topic(), "a-value")));
    }

}