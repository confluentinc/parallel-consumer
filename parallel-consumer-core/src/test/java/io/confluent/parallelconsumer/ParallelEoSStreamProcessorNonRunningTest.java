package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.TestParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
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
        final int expectedTargetLoad = 40;
        final ParallelConsumerOptions<String, String> testOptions = ParallelConsumerOptions.<String, String>builder()
                .batchSize(10)
                .maxConcurrency(2)
                .build();
        TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions);

        final int actualTargetLoad = testInstance.getTargetLoad();

        Assertions.assertEquals(expectedTargetLoad, actualTargetLoad);
    }

}
