package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;

/**
 * The options for the {@link AbstractParallelEoSStreamProcessor} system.
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
     * Path to Managed executor service for Java EE
     */
    @Builder.Default
    private final String managedExecutorService = "java:comp/DefaultManagedExecutorService";

    /**
     * Path to Managed thread factory for Java EE
     */
    @Builder.Default
    private final String managedThreadFactory = "java:comp/DefaultManagedThreadFactory";

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

        // tag::transactionalJavadoc[]
        /**
         * Periodically commits through the Producer using transactions.
         * <p>
         * The benefits of using this mode are:
         * <p>
         * a) All records produced from a given source offset will be visible, or none will be
         * ({@link org.apache.kafka.common.IsolationLevel#READ_COMMITTED}).
         * <p>
         * b) If any records making up a transaction have a terminal issue being produced, or the system crashes before
         * finishing sending all the records and committing, none will ever be visible and the system will retry th e
         * whole set in a new transaction.
         * <p>
         * c) a source offset, and it's produced records will be committed as a set. Normally: either the record
         * producing could fail, or the committing of the source offset could fail, as they are individual operations.
         * When uUsing Transactions, they are committed together - so if either operations fail, the transaction will
         * never get committed, and upon recovery the system will retry the set again (and no duplicates will be visible
         * in the topic).
         * <p>
         * Slowest of the options, but no duplicates in Kafka caused by producing a record from consuming a record, that
         * has already had its offset committed (message replay may cause duplicates in external systems which is
         * unavoidable).
         * <p>
         * Note that the system uses very large transactions (for transaction standards), by only committing by default
         * every {@link AbstractParallelEoSStreamProcessor#KAFKA_DEFAULT_AUTO_COMMIT_FREQUENCY}, which is 5 seconds.
         * This creates large transactions. However, note also that this would be the same as using non-transactional
         * committing - that being upon failure, all records not previously committed will be replayed. Reducing this
         * configuration places higher loads on the broker, but will reduce (but cannot eliminate) replay upon failure.
         * <p>
         * When producing multiple records (see {@link ParallelStreamProcessor#pollAndProduceMany}), all records must
         * have been produced successfully to the broker before the transaction will commit, after which all will be
         * visible together, or none.
         * <p>
         * Also records produced while running in this mode, won't be seen by consumer running in
         * {@link ConsumerConfig#ISOLATION_LEVEL_CONFIG} {@link org.apache.kafka.common.IsolationLevel#READ_COMMITTED}
         * mode until the transaction is complete and all records are produced successfully. Records produced into a
         * transactions that gets aborted or timed out, will never be visible.
         * <p>
         * The system must prevent records from being produced to the brokers whose source consumer record offsets has
         * not been included in this transaction. Otherwise, the transactions would include produced records from
         * consumer offsets which would only be committed in the NEXT transaction, which wouldn't make sense. To achieve
         * this, after succeeded consumer offsets are gathered, record producing is suspended, until the transaction has
         * finished committing. This periodically slows down record production during this period.
         * <p>
         * This is separate from using an IDEMPOTENT Producer, which can be used, along with
         * {@link CommitMode#PERIODIC_CONSUMER_SYNC} or {@link CommitMode#PERIODIC_CONSUMER_ASYNCHRONOUS}.
         *
         * @see AbstractParallelEoSStreamProcessor#getTimeBetweenCommits()
         */
        // end::transactionalJavadoc[]
        PERIODIC_TRANSACTIONAL_PRODUCER,

        /**
         * Periodically synchronous commits with the Consumer. Much faster than
         * {@link #PERIODIC_TRANSACTIONAL_PRODUCER}. Slower but potentially less duplicates than
         * {@link #PERIODIC_CONSUMER_ASYNCHRONOUS} upon replay.
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
     * When using {@link #getBatchSize()}, this is over and above the batch size setting. So for example, a
     * {@link #getMaxConcurrency()} of {@code 2} and a batch size of {@code 3} would result in at most {@code 15}
     * records being processed at once.
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
    private final int maxConcurrency = DEFAULT_MAX_CONCURRENCY;

    public static final int DEFAULT_MAX_CONCURRENCY = 16;

    /**
     * When a message fails, how long the system should wait before trying that message again. Note that this will not
     * be exact, and is just a target.
     */
    @Builder.Default
    private final Duration defaultMessageRetryDelay = Duration.ofSeconds(1);

    /**
     * When present, use this to generate the retry delay, instead of {@link #getDefaultMessageRetryDelay()}.
     * <p>
     * Overrides {@link #defaultMessageRetryDelay}, even if it's set.
     */
    private final Function<RecordContext<K, V>, Duration> retryDelayProvider;

    /**
     * Controls how long to block while waiting for the {@link Producer#send} to complete for any ProducerRecords
     * returned from the user-function. Only relevant if using one of the produce-flows and providing a
     * {@link ParallelConsumerOptions#producer}. If the timeout occurs the record will be re-processed in the
     * user-function.
     * <p>
     * Consider aligning the value with the {@link ParallelConsumerOptions#producer}-options to avoid unnecessary
     * re-processing and duplicates on slow {@link Producer#send} calls.
     *
     * @see org.apache.kafka.clients.producer.ProducerConfig#DELIVERY_TIMEOUT_MS_CONFIG
     */
    @Builder.Default
    private final Duration sendTimeout = Duration.ofSeconds(10);

    /**
     * Controls how long to block while waiting for offsets to be committed. Only relevant if using
     * {@link CommitMode#PERIODIC_CONSUMER_SYNC} commit-mode.
     */
    @Builder.Default
    private final Duration offsetCommitTimeout = Duration.ofSeconds(10);

    /**
     * The maximum number of messages to attempt to pass into the user functions.
     * <p>
     * Batch sizes may sometimes be less than this size, but will never be more.
     * <p>
     * The system will treat the messages as a set, so if an error is thrown by the user code, then all messages will be
     * marked as failed and be retried (Note that when they are retried, there is no guarantee they will all be in the
     * same batch again). So if you're going to process messages individually, then don't set a batch size.
     * <p>
     * Otherwise, if you're going to process messages in sub sets from this batch, it's better to instead adjust the
     * {@link ParallelConsumerOptions#getBatchSize()} instead to the actual desired size, and process them as a whole.
     * <p>
     * Note that there is no relationship between the {@link ConsumerConfig} setting of
     * {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} and this configured batch size, as this library introduces a large
     * layer of indirection between the managed consumer, and the managed queues we use.
     * <p>
     * This indirection effectively disconnects the processing of messages from "polling" them from the managed client,
     * as we do not wait to process them before calling poll again. We simply call poll as much as we need to, in order
     * to keep our queues full of enough work to satisfy demand.
     * <p>
     * If we have enough, then we actively manage pausing our subscription so that we can continue calling {@code poll}
     * without pulling in even more messages.
     * <p>
     *
     * @see ParallelConsumerOptions#getBatchSize()
     */
    @Builder.Default
    private final Integer batchSize = 1;

    /**
     * Configure the amount of delay a record experiences, before a warning is logged.
     */
    @Builder.Default
    private final Duration thresholdForTimeSpendInQueueWarning = Duration.ofSeconds(10);

    public boolean isUsingBatching() {
        return getBatchSize() > 1;
    }

    @Builder.Default
    private final int maxFailureHistory = 10;

    /**
     * @return the combined target of the desired concurrency by the configured batch size
     */
    public int getTargetAmountOfRecordsInFlight() {
        return getMaxConcurrency() * getBatchSize();
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

    public boolean isUsingTransactionalProducer() {
        return commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER);
    }

    public boolean isProducerSupplied() {
        return getProducer() != null;
    }
}
