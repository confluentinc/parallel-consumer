package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.FakeRuntimeException;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.offsets.OffsetSimultaneousEncoder;
import io.confluent.parallelconsumer.state.PartitionState;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniSets;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.truth.Truth.assertThat;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL;
import static java.lang.Integer.MAX_VALUE;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Tests the system when the input topic has transaction markers in its partitions.
 *
 * @author Antony Stubbs
 * @see PartitionState#getOffsetHighestSequentialSucceeded()
 * @see OffsetSimultaneousEncoder#OffsetSimultaneousEncoder
 */
@Tag("transactions")
@Slf4j
public class TransactionMarkersTest extends BrokerIntegrationTest<String, String> {

    /**
     * Block all records beyond the second record
     */
    final int LIMIT = 1;

    AtomicInteger receivedRecordCount = new AtomicInteger();


    Producer<String, String> txProducer;
    Producer<String, String> txProducerTwo;
    Producer<String, String> txProducerThree;
    Producer<String, String> normalProducer;
    Consumer<String, String> consumer;

    protected ParallelEoSStreamProcessor<String, String> pc;

    @BeforeEach
        // todo move to super?
    void setup() {
        setupTopic();
        consumer = getKcu().getConsumer();

        txProducer = getKcu().createAndInitNewTransactionalProducer();
        txProducerTwo = getKcu().createAndInitNewTransactionalProducer();
        txProducerThree = getKcu().createAndInitNewTransactionalProducer();

        normalProducer = getKcu().createNewProducer(NOT_TRANSACTIONAL);
        pc = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(PARTITION) // just so we dont need to use keys
                .build());
        pc.subscribe(UniSets.of(super.topic));
    }

    @AfterEach
    void close() {
        pc.close();
    }

    /**
     * Test that committing can happen successfully when the base offset for the commit is adjacent to transaction
     * markers in the input partitions.
     * <p>
     * The system assumes that the next expected (base committed) offset, will always be 1 offset above the highest
     * succeeded. But tx makers can increase this gap from 1 to potentially much higher.
     * <p>
     * todo can these gaps also be created by log compaction? If so, is the solution the same?
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/329">Github issue #329</a> the
     *         original reported issue
     */
    @Test
    void single() {
        sendOneTransaction();
        // send records - doesn't need to be in a transaction
        sendRecordsNonTransactionally(1);

        //
        runPcAndBlockRecordsOverLimitIndex();

        //
        waitForRecordsToBeReceived();

        // force commit
        // should crash now
        pc.close();
    }

    @Test
    void doubleTransaction() {
        sendOneTransaction();
        // two transactions back to back
        sendOneTransaction();

        //
        runPcAndBlockRecordsOverLimitIndex();

        //
        waitForRecordsToBeReceived();

        // force commit
        // should crash now
        pc.close();
    }

    private void waitForRecordsToBeReceived() {
        int expected = 2;
        waitForRecordsToBeReceived(expected);
    }

    private void waitForRecordsToBeReceived(int expected) {
        log.debug("Awaiting {} records to be received...", expected);
        await().untilAsserted(() -> assertThat(receivedRecordCount.get()).isAtLeast(expected));
        log.debug("Awaiting {} records to be received - done.", expected);
    }

    /**
     * Allow processing of first tx messages, but block second transaction messages
     */
    private void runPcAndBlockRecordsOverLimitIndex() {
        int blockOver = LIMIT;
        runPcAndBlockRecordsOverLimitIndex(blockOver);
    }

    private void runPcAndBlockRecordsOverLimitIndex(int blockOver) {
        pc.poll(recordContexts -> {
            int index = receivedRecordCount.incrementAndGet();
            log.debug("Got record index: {} ...", index);
            if (index > blockOver) {
                try {
                    log.debug(msg("{} over block limit of {}, blocking...", index, blockOver));
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    throw new FakeRuntimeException(e);
                }
            }
        });
    }

    private void sendOneTransaction() {
        txProducer.beginTransaction();
        txProducer.send(createRecordToSend());
        txProducer.commitTransaction();
    }

    @NotNull
    private ProducerRecord<String, String> createRecordToSend() {
        return new ProducerRecord<>(topic, "");
    }

    protected List<Future<RecordMetadata>> sendRecordsNonTransactionally(int count) {
        return IntStream.of(count).mapToObj(ignored
                        -> normalProducer.send(createRecordToSend()))
                .collect(Collectors.toList());
    }

    /**
     * @see #single()
     */
    @Test
    void several() {
        // sendSeveralTransactions, all closed at the same neighboring offsets
        sendSeveralTransaction();

        // send records - doesn't need to be in a transaction
        sendRecordsNonTransactionally(10);

        //
        runPcAndBlockRecordsOverLimitIndex();

        //
        waitForRecordsToBeReceived();

        // force commit
        pc.close(); // should crash now
    }

    private void sendSeveralTransaction() {
        IntStream.of(10).forEach(value -> sendOneTransaction());
    }

    /**
     * Allowing just the first record adjacent to the transaction marker allows the situation to proceed normally
     *
     * @see #single()
     */
    @Test
    void dontBlockFirstRecords() {
        // sendSeveralTransactions, all closed at the same neighboring offsets
        sendSeveralTransaction();

        // send records - doesn't need to be in a transaction
        sendRecordsNonTransactionally(10);

        //
        runPcAndBlockRecordsOverLimitIndex(3);

        //
        waitForRecordsToBeReceived();

        // force commit
        pc.close(); // should crash now
    }

    /**
     * @see #dontBlockFirstRecords()
     */
    @Test
    void dontBlockAnyRecords() {
        // sendSeveralTransactions, all closed at the same neighboring offsets
        sendSeveralTransaction();

        // send records - doesn't need to be in a transaction
        sendRecordsNonTransactionally(10);

        //
        runPcAndBlockRecordsOverLimitIndex(MAX_VALUE);

        //
        waitForRecordsToBeReceived();

        // force commit
        pc.close(); // should crash now
    }


    /**
     * A stacked or overlapping transaction situation, that creates 3 commit markers in a row, so we should get a wider
     * "gap" and so a more negative bitset length (-3)
     *
     * @see #single()
     */
    @Test
    void overLappingTransactions() {
        int numberOfBaseRecords = 3;

        //
        startAndOneRecord(txProducer);
        startAndOneRecord(txProducerTwo);
        startAndOneRecord(txProducerThree);

        //
        commitTx(txProducer);
        commitTx(txProducerTwo);
        commitTx(txProducerThree);

        // send records - doesn't need to be in a transaction
        sendRecordsNonTransactionally(2);

        //
        runPcAndBlockRecordsOverLimitIndex(numberOfBaseRecords);

        //
        waitForRecordsToBeReceived(numberOfBaseRecords);

        // force commit
        pc.close(); // should crash now
    }

    private void commitTx(Producer<String, String> txProducer) {
        txProducer.commitTransaction();
    }

    private void startAndOneRecord(Producer<String, String> txProducer) {
        txProducer.beginTransaction();
        txProducer.send(createRecordToSend());
    }
}
