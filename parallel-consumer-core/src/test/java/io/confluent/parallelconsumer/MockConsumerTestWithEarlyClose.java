package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Tests that PC can be closed ahead of time. Make sure PC can shut down cleanly.
 *
 * In this test, the MockConsumer will start throwing SaslAuthenticationException from 2 seconds onwards, until infinity.
 *
 * The offsetCommitTimeout as well as the saslAuthenticationRetryTimeout had been set to infinity as well.
 *
 * After 5 seconds PC will be requested to close. The expected behavior is that the PC can be shutdown cleanly.
 */
@Slf4j
@Timeout(60000L)
class MockConsumerTestWithEarlyClose {

    private final String topic = MockConsumerTestWithEarlyClose.class.getSimpleName();

    /**
     * Test that the mock consumer works as expected
     */
    @Test
    void mockConsumer() {
        final AtomicLong startFail = new AtomicLong(System.currentTimeMillis() + 2000L); // start failing after 2 seconds
        final AtomicLong failUntil = new AtomicLong(System.currentTimeMillis() + 200000000L); // never recover
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                long now = System.currentTimeMillis();
                if(now > startFail.get() && now < failUntil.get()) {
                    log.info("Mocking failure before 20 seconds");
                    throw new SaslAuthenticationException("Invalid username or password");
                }
                return super.poll(timeout);
            }

            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                long now = System.currentTimeMillis();
                if(now > startFail.get() && now < failUntil.get()) {
                    throw new SaslAuthenticationException("Invalid username or password");
                }
                super.commitSync(offsets);
            }
        };
        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(topic, 0);
        startOffsets.put(tp, 0L);

        //
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .offsetCommitTimeout(Duration.ofSeconds(10000000L))
                .saslAuthenticationRetryTimeout(Duration.ofSeconds(250000000L))
                .build();
        var parallelConsumer = new ParallelEoSStreamProcessor<String, String>(options);
        parallelConsumer.subscribe(of(topic));

        // MockConsumer is not a correct implementation of the Consumer contract - must manually rebalance++ - or use LongPollingMockConsumer
        mockConsumer.rebalance(Collections.singletonList(tp));
        parallelConsumer.onPartitionsAssigned(of(tp));
        mockConsumer.updateBeginningOffsets(startOffsets);

        //
        new Thread() {
            public void run() {
                addRecords(mockConsumer);
            }
        }.start();

        //
        ConcurrentLinkedQueue<RecordContext<String, String>> records = new ConcurrentLinkedQueue<>();
        parallelConsumer.poll(recordContexts -> {
            recordContexts.forEach(recordContext -> {
                log.warn("Processing: {}", recordContext);
                records.add(recordContext);
            });
        });
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        log.info("Trying to close...");
        parallelConsumer.close(); // request close after 5 seconds
        log.info("Close successful!");
    }

    private void addRecords(MockConsumer<String, String> mockConsumer) {
        for(int i = 0; i < 100000; i++) {
            mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, i, "key", "value"));
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
