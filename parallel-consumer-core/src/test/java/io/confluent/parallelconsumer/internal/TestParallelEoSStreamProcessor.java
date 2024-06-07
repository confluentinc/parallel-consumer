package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Provides a set of methods for testing internal and configuration based interfaces of
 * {@link AbstractParallelEoSStreamProcessor}.
 */
public class TestParallelEoSStreamProcessor<K, V> extends AbstractParallelEoSStreamProcessor<K, V> {
    public TestParallelEoSStreamProcessor(final ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);
    }

    public int getTargetLoad() { return getQueueTargetLoaded(); }

    public List<WorkContainer<K, V>> handleStaleWork(final List<WorkContainer<K, V>> workContainerBatch) {
        return super.handleStaleWork(workContainerBatch);
    }

    public void setWm(WorkManager wm) {
        super.wm = wm;
    }

    public BlockingQueue getMailBox() {
        return super.getWorkMailBox();
    }

}
