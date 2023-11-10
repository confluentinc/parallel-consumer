package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.TestParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests that the protected and internal methods of
 * {@link io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor} work as expected.
 * <p>
 *
 * @author Jonathon Koyle
 */
@Slf4j
class ParallelEoSStreamProcessorNonRunningTest {

    /**
     * Test that the mock consumer works as expected
     */
    @Test
    void getTargetLoad() {
        final int batchSize = 10;
        final int concurrency = 2;
        final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        final ParallelConsumerOptions<String, String> testOptions = ParallelConsumerOptions.<String, String>builder()
                .batchSize(batchSize)
                .maxConcurrency(concurrency)
                .consumer(consumer)
                .build();
        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            final int defaultLoad = 2;
            final int expectedTargetLoad = batchSize * concurrency * defaultLoad;

            final int actualTargetLoad = testInstance.getTargetLoad();

            Assertions.assertEquals(expectedTargetLoad, actualTargetLoad);
        }
    }
}
