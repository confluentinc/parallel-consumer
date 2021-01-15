package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;

/**
 * The options for the {@link ParallelEoSStreamProcessor} system.
 *
 * @see #builder()
 * @see ParallelConsumerOptions.ParallelConsumerOptionsBuilder
 */
@Getter
@Builder(toBuilder = true)
@ToString
public class ParallelConsumerOptions<K, V> {

    /**
     * Required parameter for all use.
     */
    private final Consumer<K, V> consumer;

    /**
     * Supplying a producer is only needed if using the produce flows.
     *
     * @see ParallelStreamProcessor
     */
    private final Producer<K, V> producer;

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
         * Periodically commits through the Producer using transactions. Slowest of the options, but no duplicates in
         * Kafka guaranteed (message replay may cause duplicates in external systems which is unavoidable with Kafka).
         * <p>
         * This is separate from using an IDEMPOTENT Producer, which can be used, along with {@link
         * CommitMode#PERIODIC_CONSUMER_SYNC} or {@link CommitMode#PERIODIC_CONSUMER_ASYNCHRONOUS}.
         */
        PERIODIC_TRANSACTIONAL_PRODUCER,

        /**
         * Periodically synchronous commits with the Consumer. Much faster than {@link
         * #PERIODIC_TRANSACTIONAL_PRODUCER}. Slower but potentially less duplicates than {@link
         * #PERIODIC_CONSUMER_ASYNCHRONOUS} upon replay.
         */
        PERIODIC_CONSUMER_SYNC,

        /**
         * Periodically commits offsets asynchronously. The fastest option, under normal conditions will have few or no
         * duplicates. Under failure recovery may have more duplicates than {@link #PERIODIC_CONSUMER_SYNC}.
         */
        PERIODIC_CONSUMER_ASYNCHRONOUS

    }

    /**
     * The {@link ProcessingOrder} type to use
     */
    @Builder.Default
    private final ProcessingOrder ordering = ProcessingOrder.KEY;

    /**
     * The {@link CommitMode} to be used
     */
    @Builder.Default
    private final CommitMode commitMode = CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;

    /**
     * Controls the maximum degree of concurrency to occur. Used to limit concurrent calls to external systems to a
     * maximum to prevent overloading them or to a degree, using up quotas.
     * <p>
     * A note on quotas - if your quota is expressed as maximum concurrent calls, this works well. If it's limited in
     * total requests / sec, this may still overload the system. See towards the distributed rate limiting feature for
     * this to be properly addressed: https://github.com/confluentinc/parallel-consumer/issues/24 Add distributed rate
     * limiting support #24.
     * <p>
     * In the core module, this sets the number of threads to use in the core's thread pool.
     * <p>
     * It's recommended to set this quite high, much higher than core count, as it's expected that these threads will
     * spend most of their time blocked waiting for IO. For automatic setting of this variable, look out for issue
     * https://github.com/confluentinc/parallel-consumer/issues/21 Dynamic concurrency control with flow control or tcp
     * congestion control theory #21.
     */
    @Builder.Default
    private final int maxConcurrency = 16;

    /**
     * When a message fails, how long the system should wait before trying that message again.
     */
    @Builder.Default
    private final Duration defaultMessageRetryDelay = Duration.ofSeconds(1);

    /**
     * The maximum number of messages to attempt pass into the {@link ParallelConsumer#pollBatch} user function. Batch
     * sizes may be less than this size, but will never be more.
     * <p>
     * Note that there is no relationship between the Consumer setting of {@code max.poll.records} and this configured
     * batch size, as this library introduces a large layer of indirection between the managed consumer, and the managed
     * queues we use. This indirection effectively disconnects the processing of messages from "polling" them from the
     * managed client, as we do not wait to process them before calling poll again. We simply call poll as much as we
     * need to in order to keep our queues full with enough work to satisfy demand - and if we have enough then we
     * actively mange pausing our subscription so that we can continue calling {@code poll} without pulling in even more
     * messages.
     */
    private final Integer batchSize;

    /**
     * @see #batchSize
     */
    public Optional<Integer> getBatchSize() {
        return Optional.ofNullable(batchSize);
    }

    public void validate() {
        Objects.requireNonNull(consumer, "A consumer must be supplied");

        if (isUsingTransactionalProducer() && producer == null) {
            throw new IllegalArgumentException(msg("Wanting to use Transaction Producer mode ({}) without supplying a Producer instance",
                    commitMode));
        }

        //
        WorkContainer.setDefaultRetryDelay(getDefaultMessageRetryDelay());
    }

    protected boolean isUsingTransactionalProducer() {
        return commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER);
    }

    public boolean isProducerSupplied() {
        return getProducer() != null;
    }
}
