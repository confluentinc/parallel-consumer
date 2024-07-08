package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Provides a set of methods for testing internal and configuration based interfaces of
 * {@link AbstractParallelEoSStreamProcessor}.
 */
public class TestParallelEoSStreamProcessor<K, V> extends AbstractParallelEoSStreamProcessor<K, V> {
    public TestParallelEoSStreamProcessor(final ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);
    }

    public int getTargetLoad() { return getQueueTargetLoaded(); }

    public  <R> List<Tuple<ConsumerRecord<K, V>, R>> runUserFunc(
            Function<PollContextInternal<K, V>, List<R>> dummyFunction,
            Consumer<R> callback,
            final List<WorkContainer<K, V>> activeWorkContainers) {

        return super.runUserFunction(dummyFunction, callback , activeWorkContainers);
    }

    public void setWm(WorkManager wm) {
        super.wm = wm;
    }

    public long getMailBoxSuccessCnt() {
        return super.getWorkMailBox().stream()
                .filter(kvControllerEventMessage -> {
                    WorkContainer<K, V> wc = kvControllerEventMessage.getWorkContainer();
                    return (wc != null && wc.isUserFunctionSucceeded());
                })
                .count();
    }

    public long getMailBoxFailedCnt() {
        return super.getWorkMailBox().stream()
                .filter(kvControllerEventMessage -> {
                    WorkContainer<K, V> wc = kvControllerEventMessage.getWorkContainer();
                    return (wc != null && !wc.isUserFunctionSucceeded());
                })
                .count();
    }

    public List<List<V>> partitionList(List<V> sourceCollection, int maxBatchSize) {
        return partition(sourceCollection, maxBatchSize);
    }
}
