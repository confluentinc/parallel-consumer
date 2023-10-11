package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.ShardKey;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class RetryHandler<K, V> implements Runnable {
    private final BlockingQueue<WorkContainer<K, V>> retryQueue;

    private AtomicLong retryItemCnt;

    private final PCModule<K, V> pc;

    private boolean isStopped;

    // timestamp to be checked how long to check the heap and increase the availableWorkContainerCnt
    private long dueMillis = Long.MAX_VALUE;

    public RetryHandler(PCModule<K, V> pc) {
        this.pc = pc;
        retryQueue = pc.workManager().getSm().getRetryQueue();
        retryItemCnt = pc.workManager().getSm().getRetryItemCnt();
        isStopped = false;
    }

    @Override
    public void run() {
        while (!isStopped) {
            updateDueMillis();
            // if there is already failed task to be retried, then wait for the timing to reduce the IO
            if (isTimeForRetry()) {
                pollRetryQueueToAvailableWorkerMap();
            }
        }
    }

    public void close() {
        isStopped = true;
    }


    // if the worker is ready to be
    private boolean isTimeForRetry() {
        return dueMillis - pc.clock().millis() <= 0;
    }

    private void updateDueMillis() {
        // only check retryQueue if there is no candidates to retry
        if (dueMillis == Long.MAX_VALUE && retryItemCnt.get() > 0) {
            WorkContainer<K, V> wc = retryQueue.peek();
            if (wc != null) {
                dueMillis = wc.getRetryDueAt().toEpochMilli();
            }
        }
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

        // reset the timestamp
        dueMillis = Long.MAX_VALUE;
    }
}
