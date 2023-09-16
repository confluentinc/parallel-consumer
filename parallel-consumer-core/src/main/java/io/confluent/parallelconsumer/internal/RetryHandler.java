package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.ShardKey;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;

@Slf4j
public class RetryHandler<K, V> implements Runnable {
    private final BlockingQueue<WorkContainer<K, V>> retryQueue;

    private final PCModule<K, V> pc;

    public RetryHandler(PCModule<K, V> pc) {
        this.pc = pc;
        retryQueue = pc.workManager().getSm().getRetryQueue();
    }

    @Override
    public void run() {
        if (isTimeForRetry()) {
            pollRetryQueueToAvailableWorkerMap();
        }
    }

    // if the worker is ready to be
    private boolean isTimeForRetry() {
        WorkContainer<K, V> wc = retryQueue.peek();

        return wc != null && wc.getRetryDueAt().toEpochMilli() - pc.clock().millis() <= 0;
    }


    // poll retry queue records updates the available count
    private void pollRetryQueueToAvailableWorkerMap() {
        // could be race condition with getTimeToBlockFor() when it tries to get the earliest worker
        // but since the block time also related to commit interval, so this impact should be trivial
        WorkContainer<K, V> wc = retryQueue.poll();
        if (wc != null) {
            ShardKey shardKey = pc.workManager().getSm().computeShardKey(wc);
            pc.workManager().getSm().getProcessingShards().computeIfPresent(shardKey, (k ,v) -> v.incrAvailableWorkContainerCnt());
        }
    }
}
