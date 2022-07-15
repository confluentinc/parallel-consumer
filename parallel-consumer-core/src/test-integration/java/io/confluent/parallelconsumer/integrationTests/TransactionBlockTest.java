package io.confluent.parallelconsumer.integrationTests;

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ConsumerGroupId;
import lombok.NonNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
class TransactionBlockTest extends TransactionMarkersTest {

    @Test
    void testProducerLock() {
        // send source messages
        int numRecords = 3;
        int blockFreeRecords = numRecords - 1;
        int blockedOffset = numRecords - 1;
        sendRecordsNonTransactionallyAndBlock(numRecords);

        var blockedRec = new CountDownLatch(1);

        // process two records, sending 3 from each
        pc.pollAndProduceMany(recordContexts -> {
            if (recordContexts.offset() == blockedOffset) {
                // todo use waiter
                try {
                    blockedRec.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            return makeOutput(recordContexts);
        });

        // start committing transaction
        pc.requestCommitAsap();

        // assert tx completes
        var isolationCommittedConsumer = kcu.createNewConsumer(ConsumerGroupId.NEW);
        {
            var poll = isolationCommittedConsumer.poll(Duration.ZERO);
            assertThat(poll).containsOffset(blockFreeRecords);
        }

        // while sending tx, try to produce another record, observe it's blocked
        // unblock
        blockedRec.countDown();
        // assert for 1 second
        pc.requestCommitAsap();
        {
            var poll = isolationCommittedConsumer.poll(Duration.ZERO);
            assertThat(poll).doesntContainOffset(blockedOffset);
        }

        // finish transaction
//        ???
        Truth.assertThat(true).isFalse();

        // assert blocked record now sent
        pc.requestCommitAsap();
        assertThat(pc).hasCommittedToAnything(blockFreeRecords);
        {
            var poll = isolationCommittedConsumer.poll(Duration.ZERO);
            assertThat(poll).containsOffset(blockedOffset);
        }

        // commit open transaction
//        ???
        Truth.assertThat(true).isFalse();

        // assert results topic contains all
        assertThat(pc).hasCommittedToAnything(blockFreeRecords);
        {
            var poll = isolationCommittedConsumer.poll(Duration.ZERO);
            assertThat(poll).containsOffset(blockedOffset);
        }
    }

    @Test
    void abortedTransaction() {
        // do the above again, but instead abort the transaction
        // assert nothing on result topic
        // retry
        // assert results in output topic
        Truth.assertThat(true).isFalse();
    }

    @NonNull
    private List<ProducerRecord<String, String>> makeOutput(PollContext<String, String> recordContexts) {
        return recordContexts.stream()
                .map(record
                        -> new ProducerRecord<String, String>(topic, record.value()))
                .collect(Collectors.toList());
    }

    private void sendRecordsNonTransactionallyAndBlock(int i) {
        sendRecordsNonTransactionally(3).forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
