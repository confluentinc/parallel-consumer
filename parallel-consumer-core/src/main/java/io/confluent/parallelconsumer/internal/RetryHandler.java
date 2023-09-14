package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.ShardKey;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class RetryHandler<K, V> implements Runnable {
    private BlockingQueue<WorkContainer<K, V>> retryQueue;
    private State state;

    private long lastRetryMillis;

    private PCModule<K, V> pc;

    private final static long DEFAULT_WAITING_MILLIS = 5L;
    private final static long DEFAULT_MARGIN_MILLIS = 5L;

    public RetryHandler(PCModule<K, V> pc) {
        this.pc = pc;
        retryQueue = pc.workManager().getSm().getRetryQueue();
        state = pc.pc().getState();
    }

    @Override
    public void run() {
        while (state != State.CLOSED) {
            long waitingMillis = getRetryWaitingMillis();
            if (waitingMillis <= 0) {
                pollRetryQueueToAvailableWorkerMap();
            }
        }
    }

    private long getRetryWaitingMillis() {
        WorkContainer<K, V> wc = retryQueue.peek();

        return wc != null ? wc.getRetryDueAt().toEpochMilli() - pc.clock().millis() : DEFAULT_WAITING_MILLIS;
    }

    private boolean reachTiming() {
         return System.currentTimeMillis() - lastRetryMillis > 5000L;
    }

    // poll retry queue records to mailbox queue to be processed
    // the retry queue modifications are all happening in the same thread, no need to worry about race condition
    private void pollRetryQueueToAvailableWorkerMap() {
        WorkContainer<K, V> wc = retryQueue.poll();
        if (wc != null) {
            ShardKey shardKey = pc.workManager().getSm().computeShardKey(wc);
            pc.workManager().getSm().getProcessingShards().get(shardKey).incrAvailableWorkContainerCnt();
        }
    }
}
