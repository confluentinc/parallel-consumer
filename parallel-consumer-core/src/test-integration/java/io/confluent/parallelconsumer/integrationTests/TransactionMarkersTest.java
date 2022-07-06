package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.offsets.OffsetSimultaneousEncoder;
import io.confluent.parallelconsumer.state.PartitionState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.google.common.truth.Truth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.NORMAL;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Tests the system when the input topic has transaction markers in its partitions.
 *
 * @author Antony Stubbs
 * @see PartitionState#getOffsetHighestSequentialSucceeded()
 * @see OffsetSimultaneousEncoder
 */
class TransactionMarkersTest extends BrokerIntegrationTest<String, String> {

    /**
     * Block all records beyond the second record
     */
    private static final int LIMIT = 2;

    Producer<String, String> txProducer;
    Producer<String, String> normalProducer;
    Consumer<String, String> consumer;
    ParallelEoSStreamProcessor<String, String> pc;

    @BeforeEach
        // todo move to super?
    void setup() {
        setupTopic();
        consumer = getKcu().getConsumer();
        txProducer = getKcu().createNewTransactionalProducer();
        normalProducer = getKcu().createNewProducer(NORMAL);
        pc = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(PARTITION) // just so we dont need to use keys
                .build());
        pc.subscribe(Set.of(super.topic));
    }

    @AfterEach
    void close() {
        pc.close();
    }

    /**
     * Originally written to test issue# XXX - that committing can happen successfully when the base offset for the
     * commit is adjacent to transaction markers in the input partitions.
     * <p>
     * The system assumes that the next expected (base committed) offset, will always be 1 offset above the highest
     * succeeded. But tx makers can increase this gap from 1 to potentially much higher.
     * <p>
     * todo can these gaps also be created by log compaction? If so, is the solution the same?
     */
    @Test
    void single() {
        sendOneTransaction();
        // send records - doesn't need to be in a transaction
        sendRecordsNonTransactionally(1);
        blockRecordsOverLimitIndex();
        await().untilAsserted(() -> assertThat(receivedRecordCount.get()).isEqualTo(4));
        // force commit
        // should crash now
        Assertions.assertThatThrownBy(() -> pc.close()).isInstanceOf(Exception.class);
    }

    AtomicInteger receivedRecordCount = new AtomicInteger();

    /**
     * Allow processing of first tx messages, but block second transaction messages
     */
    private void blockRecordsOverLimitIndex() {
        pc.poll(recordContexts -> {
            int index = receivedRecordCount.incrementAndGet();
            if (index > LIMIT) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void sendOneTransaction() {
        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topic, ""));
        txProducer.commitTransaction();
    }

    private void sendRecordsNonTransactionally(int count) {
        IntStream.of(count).forEach(i -> normalProducer.send(new ProducerRecord<>(topic, "")));
    }

    /**
     * @see #single()
     */
    @Test
    void several() {
        // sendSeveralTransactions, all closed at the same neighboring offsets
        sendSeveralTransaction();
        // send records - doesn't need to be in a transaction
        sendRecordsNonTransactionally(1);
        blockRecordsOverLimitIndex();
        // force commit
        pc.close(); // should crash now
    }

    private void sendSeveralTransaction() {
        IntStream.of(10).forEach(value -> sendOneTransaction());
    }
}
