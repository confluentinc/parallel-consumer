package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.StringUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.Properties;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.TRANSACTIONAL_PRODUCER;

/**
 * The options for the {@link ParallelEoSStreamProcessor} system.
 */
@Getter
@Builder(toBuilder = true)
@ToString
public class ParallelConsumerOptions {

    /**
     * The ordering guarantee to use.
     */
    public enum ProcessingOrder {
        /**
         * No ordering is guaranteed, not even partition order. Fastest. Concurrency is at most the max number of
         * concurrency or max number of uncommitted messages, limited by the max concurrency or uncommitted settings.
         */
        UNORDERED,
        /**
         * Process messages within a partition in order, but process multiple partitions in parallel. Similar to running
         * more consumer for a topic. Concurrency is at most the number of partitions.
         */
        PARTITION,
        /**
         * Process messages in key order. Concurrency is at most the number of unique keys in a topic, limited by the
         * max concurrency or uncommitted settings.
         */
        KEY
    }

    /**
     * The type of commit to be made, with either a transactions configured Producer where messages produced are
     * committed back to the Broker along with the offsets they originated from, or with the faster simpler Consumer
     * offset system either synchronously or asynchronously
     */
    public enum CommitMode {
        /**
         * Commits through the Producer using transactions. Slowest fot he options, but no duplicates in Kafka
         * guaranteed (message replay may cause duplicates in external systems which is unavoidable with Kafka).
         */
        TRANSACTIONAL_PRODUCER,
        /**
         * Synchronous commits with the Consumer. Much faster than {@link #TRANSACTIONAL_PRODUCER}. Slower but
         * potentially less duplicates than {@link #CONSUMER_ASYNCHRONOUS} upon replay.
         */
        CONSUMER_SYNC,
        /**
         * Fastest option, under normal conditions will have few of no duplicates. Under failure revocery may have more
         * duplicates than {@link #CONSUMER_SYNC}.
         */
        CONSUMER_ASYNCHRONOUS
    }

    /**
     * The order type to use
     */
    @Builder.Default
    private final ProcessingOrder ordering = ProcessingOrder.UNORDERED;

    @Builder.Default
    private final CommitMode commitMode = CommitMode.CONSUMER_ASYNCHRONOUS;

    /**
     * When using a produce flow, when producing the message, either
     * <p>
     * This is separate from using an IDEMPOTENT Producer, which can be used, along with {@link
     * CommitMode#CONSUMER_SYNC} or {@link CommitMode#CONSUMER_ASYNCHRONOUS}.
     * <p>
     * Must be set to true if being used with a transactional producer. Producers state. However, forcing it to be
     * specified makes the choice more verbose?
     * <p>
     * TODO we could just auto detect this from the
     * <p>
     * TODO delete in favor of CommitMode
     */
    @Builder.Default
    private final boolean usingTransactionalProducer = false;

    /**
     * Don't have more than this many uncommitted messages in process
     * <p>
     * TODO docs - needs renaming. Messages aren't "uncommitted", they're just in the comitted offset map instead of the
     * committed base offset
     */
    @Builder.Default
    private final int maxUncommittedMessagesToHandle = 1000;

    /**
     * Don't process any more than this many messages concurrently
     * <p>
     * TODO docs differentiate from thread count, vertx etc. remove to vertx module?
     */
    @Builder.Default
    private final int maxConcurrency = 100;

    /**
     * TODO docs. rename to max concurrency. differentiate between this and vertx threads
     */
    @Builder.Default
    private final int numberOfThreads = 16;

    // TODO remove - or include when checking passed property arguments instead of instances
    //    using reflection instead
//    @Builder.Default
//    private final Properties producerConfig = new Properties();

    public void validate() {
        boolean commitModeIsTx = commitMode.equals(TRANSACTIONAL_PRODUCER);
        if (this.isUsingTransactionalProducer() ^ commitModeIsTx) {
            throw new IllegalArgumentException(msg("Using transaction producer mode ({}) without matching commit mode ({})",
                    this.isUsingTransactionalProducer(),
                    commitMode));
        }
    }
}
