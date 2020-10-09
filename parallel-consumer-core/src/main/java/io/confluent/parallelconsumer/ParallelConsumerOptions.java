package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.Builder;
import lombok.Getter;

/**
 * The options for the {@link ParallelEoSStreamProcessor} system.
 */
@Getter
@Builder
public class ParallelConsumerOptions {

    public enum ProcessingOrder {
        /**
         * No ordering is guaranteed, not even partition order. Fastest. Concurrency is at most the max number of
         * concurrency or max number of uncommitted messages, limited by the max concurrency or uncommitted settings.
         */
        UNORDERED,
        /**
         * Process messages within a partition in order, but process multiple partitions in parallel. Similar to
         * running more consumer for a topic. Concurrency is at most the number of partitions.
         */
        PARTITION,
        /**
         * Process messages in key order. Concurrency is at most the number of unique keys in a topic, limited by the
         * max concurrency or uncommitted settings.
         */
        KEY
    }

    /**
     * The order type to use
     */
    @Builder.Default
    private final ProcessingOrder ordering = ProcessingOrder.UNORDERED;

    /**
     * Don't have more than this many uncommitted messages in process
     * TODO change this to per topic? global?
     */
    @Builder.Default
    private final int maxUncommittedMessagesToHandlePerPartition = 1000;

    /**
     * Don't process any more than this many messages concurrently
     */
    @Builder.Default
    private final int maxConcurrency = 100;

    @Builder.Default
    private final int numberOfThreads = 16;


}
