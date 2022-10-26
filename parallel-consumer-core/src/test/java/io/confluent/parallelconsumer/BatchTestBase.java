package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

public interface BatchTestBase {

    void averageBatchSizeTest();

    /**
     * Use:
     *
     * @ParameterizedTest
     * @EnumSource
     */
    void simpleBatchTest(ParallelConsumerOptions.ProcessingOrder order);

    /**
     * Use:
     *
     * @ParameterizedTest
     * @EnumSource
     */
    void batchFailureTest(ParallelConsumerOptions.ProcessingOrder order);
}
