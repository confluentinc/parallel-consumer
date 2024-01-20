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

    private final AtomicLong retryItemCnt;

    private final PCModule<K, V> pc;

    private boolean isStopped;

    // timestamp to be checked how long to check the heap and increase the availableWorkContainerCnt
    private long nextRetryTimestampMs;

    // if there are retry worker waiting to be removed when retry time is due
    private boolean inWaitingTobePolled = false;

    public RetryHandler(PCModule<K, V> pc) {
        this.pc = pc;
        retryQueue = pc.workManager().getSm().getRetryQueue();
        retryItemCnt = pc.workManager().getSm().getRetryItemCnt();
        isStopped = false;
    }

    @Override
    public void run() {
        while (!isStopped) {
            updateNextRetryTimestampMs();
            // if there is already failed task to be retried, then wait for the timing to reduce the IO
            if (isTimeForRetry()) {
                pollFromRetryQueue();
            }
        }
    }

    public void execute() {
        updateNextRetryTimestampMs();
        // if there is already failed task to be retried, then wait for the timing to reduce the IO
        if (isTimeForRetry()) {
            pollFromRetryQueue();
        }
    }

    public void close() {
        isStopped = true;
    }

    private boolean isTimeForRetry() {
        return inWaitingTobePolled && nextRetryTimestampMs - pc.clock().millis() <= 0;
    }

    private void updateNextRetryTimestampMs() {
        // only check retryQueue if there is no candidates to retry and there are items in the retryQueue
        if (!inWaitingTobePolled && retryItemCnt.get() > 0) {
            WorkContainer<K, V> wc = retryQueue.peek();
            if (wc != null) {
                nextRetryTimestampMs = wc.getRetryDueAt().toEpochMilli();
                inWaitingTobePolled = true;
            }
        }
    }


    // poll retry queue records updates the available count
    private void pollFromRetryQueue() {
        // could be race condition with getTimeToBlockFor() when it tries to get the earliest worker
        // but since the block time only related to commit interval, so this impact should be trivial
        WorkContainer<K, V> wc = retryQueue.poll();
        if (wc != null) {
            ShardKey shardKey = pc.workManager().getSm().computeShardKey(wc);
            pc.workManager().getSm().getProcessingShards().computeIfPresent(shardKey, (k ,v) -> v.incrAvailableWorkContainerCnt());
            retryItemCnt.decrementAndGet();
        }

        // reset the flag
        inWaitingTobePolled = false;
    }
}
