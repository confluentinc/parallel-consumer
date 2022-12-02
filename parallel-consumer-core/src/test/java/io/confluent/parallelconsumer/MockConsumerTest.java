package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.LongPollingMockConsumer;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Tests that PC works fine with the plain vanilla {@link MockConsumer}, as opposed to the
 * {@link LongPollingMockConsumer}.
 * <p>
 * These tests demonstrate why using {@link MockConsumer} is difficult, and why {@link LongPollingMockConsumer} should
 * be used instead.
 *
 * @author Antony Stubbs
 * @see LongPollingMockConsumer#revokeAssignment
 */
@Slf4j
class MockConsumerTest {

    private final String topic = MockConsumerTest.class.getSimpleName();

    /**
     * Test that the mock consumer works as expected
     */
    @Test
    void mockConsumer() {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(topic, 0);
        startOffsets.put(tp, 0L);

        //
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .build();
        var module = new PCModuleTestEnv(options);
        try (var parallelConsumer = new ParallelEoSStreamProcessor<String, String>(options, module);) {
            parallelConsumer.subscribe(of(topic));

            // MockConsumer is not a correct implementation of the Consumer contract - must manually rebalance++ - or use LongPollingMockConsumer
            mockConsumer.rebalance(Collections.singletonList(tp));
            module.rebalanceHandler().onPartitionsAssigned(of(tp));
            mockConsumer.updateBeginningOffsets(startOffsets);

            //
            addRecords(mockConsumer);

            //
            ConcurrentLinkedQueue<RecordContext<String, String>> records = new ConcurrentLinkedQueue<>();
            parallelConsumer.poll(recordContexts -> {
                recordContexts.forEach(recordContext -> {
                    log.warn("Processing: {}", recordContext);
                    records.add(recordContext);
                });
            });

            //
            Awaitility.await().untilAsserted(() -> {
                assertThat(records).hasSize(3);
            });
        }
    }

    private void addRecords(MockConsumer<String, String> mockConsumer) {
        mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, 0, "key", "value"));
        mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, 1, "key", "value"));
        mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, 2, "key", "value"));
    }

}
