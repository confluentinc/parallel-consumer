package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Objects;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.TRANSACTIONAL_PRODUCER;

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

    private final Consumer<K, V> consumer;

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
         * Commits through the Producer using transactions. Slowest fot he options, but no duplicates in Kafka
         * guaranteed (message replay may cause duplicates in external systems which is unavoidable with Kafka).
         * <p>
         * This is separate from using an IDEMPOTENT Producer, which can be used, along with {@link
         * CommitMode#CONSUMER_SYNC} or {@link CommitMode#CONSUMER_ASYNCHRONOUS}.
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
     * Total max number of messages to process beyond the base committed offsets.
     * <p>
     * This acts as a sort sort of upper limit on the number of messages we should allow our system to handle, when
     * working with large quantities of messages that haven't been included in the normal Broker offset commit protocol.
     * I.e. if there is a single message that is failing to process, without this limit we will continue on forever with
     * our system, with the actual (normal) committed offset never moving, and relying totally on our {@link
     * OffsetMapCodecManager} to encode the process status of our records and store in metadata next to the committed
     * offset.
     * <p>
     * At the moment this is a sort of sanity check, and was chosen rather arbitriarly. However, one should consider
     * that this is per client, and is a total across all assigned partitions.
     */
    @Builder.Default
    private final int maxNumberMessagesBeyondBaseCommitOffset = 1000;

    /**
     * Max number of messages to queue up in our execution system and attempt to process concurrently.
     * <p>
     * In the core module, this will be constrained by the {@link #numberOfThreads} setting, as that is the max actual
     * concurrency for processing the messages. To actually get this degree of concurrency, you would need to have a
     * matching number of threads in the pool.
     * <p>
     * However with the VertX engine, this will control how many messages at a time are being submitted to the Vert.x
     * engine to process. As Vert.x isn't constrained by a thread count, this will be the actual degree of concurrency.
     */
    @Builder.Default
    private final int maxMessagesToQueue = 100;

    /**
     * Number of threads to use in the core's thread pool.
     */
    @Builder.Default
    private final int numberOfThreads = 16;

    public void validate() {
        Objects.requireNonNull(consumer, "A consumer must be supplied");

        if (isUsingTransactionalProducer() && producer == null) {
            throw new IllegalArgumentException(msg("Wanting to use Transaction Producer mode ({}) without supplying a Producer instance",
                    commitMode));
        }
    }

    protected boolean isUsingTransactionalProducer() {
        return commitMode.equals(TRANSACTIONAL_PRODUCER);
    }

    public boolean isProducerSupplied() {
        return getProducer() != null;
    }
}
