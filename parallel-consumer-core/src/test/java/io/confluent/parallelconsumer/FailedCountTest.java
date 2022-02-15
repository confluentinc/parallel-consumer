package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;

class FailedCountTest extends ParallelEoSStreamProcessorTestBase {

    @Test
    void failedCountTest() {
        primeFirstRecord();

        AtomicInteger iteration = new AtomicInteger();
        AtomicReference<ConsumerRecord> lastRec = new AtomicReference<>();
        parallelConsumer.poll(consumerRecord -> {
            iteration.incrementAndGet();

            lastRec.set(consumerRecord);
            int failedCount = ParallelConsumerRecord.getFailedCount(consumerRecord);

            if (iteration.get() == 1) {
                assertThat(failedCount).isEqualTo(0);
                throw new RuntimeException("fail 1");
            } else if (iteration.get() == 2) {
                assertThat(failedCount).isEqualTo(1);
            }
        });
        waitForOneLoopCycle();
        parallelConsumer.closeDrainFirst();

        ConsumerRecord consumerRecord = lastRec.get();
        assertThat(consumerRecord).isNotNull();
        int failedCount = ParallelConsumerRecord.getFailedCount(consumerRecord);
        assertThat(failedCount).isEqualTo(1);
    }
}
