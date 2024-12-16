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
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Tests that PC works fine with a consumer where the commitSync fails with TimeoutException after 5 seconds.
 *
 * After the first 20 seconds, commitSync will resume normal behavior: Succeed immediately
 *
 * In this test, we want to make sure the PC still resumes normal operation after several TimeoutException on commitSync timeout.
 * @author Shilin Wu
 */
@Slf4j
@Timeout(60000L)
class MockConsumerTestWithCommitTimeoutException {

    private final String topic = MockConsumerTestWithCommitTimeoutException.class.getSimpleName();

    /**
     * Test that the PC can resume operation after several failures
     */
    @Test
    void mockConsumer() {
        final AtomicLong failUntil = new AtomicLong(System.currentTimeMillis() + 20000L);
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                // polls are normal
                return super.poll(timeout);
            }

            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                // fail with timeout after 5 seconds for the first 20 seconds
                if(System.currentTimeMillis() < failUntil.get()) {
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new TimeoutException("Timeout after 5 seconds (mocking)");
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
                .offsetCommitTimeout(Duration.ofSeconds(25L)) // commit timeout set to 25 seconds
                .commitInterval(Duration.ofSeconds(1L)) // commit interval set to 1 second
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC) // use sync commit
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

        // temporarily set the wait timeout
        Awaitility.setDefaultTimeout(Duration.ofSeconds(50));
        //
        Awaitility.await().untilAsserted(() -> {
            assertThat(records).hasSize(10);
        });

        Awaitility.reset();
    }

    private void addRecords(MockConsumer<String, String> mockConsumer) {
        for(int i = 0; i < 10; i++) {
            mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, i, "key", "value"));
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
