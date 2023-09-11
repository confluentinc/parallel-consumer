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
    private BlockingQueue<AbstractParallelEoSStreamProcessor.ControllerEventMessage<K, V>> workMailBox;
    private BlockingQueue<WorkContainer<K, V>> retryQueue;

    private BlockingQueue<WorkContainer<K, V>> targetRetryQueue = new ArrayBlockingQueue<>(100000);
    private State state;

    private long lastRetryMillis;

    private PCModule<K, V> pc;

    private final static long DEFAULT_WAITING_MILLIS = 5000L;
    private final static long DEFAULT_MARGIN_MILLIS = 500L;

    public RetryHandler(PCModule<K, V> pc) {
        this.pc = pc;
        workMailBox = pc.pc().getWorkMailBox();
        retryQueue = pc.workManager().getSm().getRetryQueue();
        state = pc.pc().getState();
    }

    @Override
    public void run() {
        while (state != State.CLOSED) {
            long waitingMillis = getRetryWaitingMillis() + DEFAULT_MARGIN_MILLIS;
            if (waitingMillis <= 0) {
                pollRetryQueueToAvailableWorkerMap();
            } else {
                try {
                    Thread.sleep(waitingMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private long getRetryWaitingMillis() {
        WorkContainer<K, V> wc = retryQueue.peek();

        return wc != null ? wc.getRetryDueAt().toEpochMilli() - Instant.now().toEpochMilli() : DEFAULT_WAITING_MILLIS;
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
            pc.workManager().getSm().getProcessingShards().get(shardKey).addWorkContainerToAvailableContainers(wc);
        }

//        BlockingQueue<AbstractParallelEoSStreamProcessor.ControllerEventMessage<K, V>> q = new ArrayBlockingQueue<>(10000);
//
//        for (; wc != null; wc = retryQueue.poll()) {
//            log.debug("poll retry queue records to mailbox queue to be processed");
//            q.add(AbstractParallelEoSStreamProcessor.ControllerEventMessage.of(wc));
//        }
//        if (!q.isEmpty()) {
//            q.drainTo(workMailBox);
//            lastRetryMillis = System.currentTimeMillis();
//        }

    }
}
