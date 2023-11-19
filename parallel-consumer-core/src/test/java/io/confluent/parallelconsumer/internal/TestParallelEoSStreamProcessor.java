package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;

/**
 * Provides a set of methods for testing internal and configuration based interfaces of
 * {@link AbstractParallelEoSStreamProcessor}.
 */
public class TestParallelEoSStreamProcessor<K, V> extends AbstractParallelEoSStreamProcessor<K, V> {
    public TestParallelEoSStreamProcessor(final ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);
    }

    public int getTargetLoad() { return getQueueTargetLoaded(); }
}
