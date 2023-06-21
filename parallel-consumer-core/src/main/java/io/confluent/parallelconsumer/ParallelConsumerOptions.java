package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldNameConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static java.time.Duration.ofMillis;

/**
 * The options for the {@link AbstractParallelEoSStreamProcessor} system.
 * <p>
 * The important options to look at are:
 * <p>
 * {@link #ordering}, {@link #maxConcurrency} and {@link #batchSize}.
 * <p>
 * If you want to go deeper, look at {@link #defaultMessageRetryDelay}, {@link #retryDelayProvider} and
 * {@link #commitMode}.
 * <p>
 * Note: The only required option is the {@link #consumer} ({@link #producer} is only needed if you use the Produce
 * flows). All other options have sensible defaults.
 *
 * @author Antony Stubbs
 * @see #builder()
 * @see ParallelConsumerOptions.ParallelConsumerOptionsBuilder
 */
@Getter
@Builder(toBuilder = true)
@ToString
@FieldNameConstants
@InterfaceStability.Evolving
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
         * Messages sent in parallel by different workers get added to the same transaction block - you end up with
         * transactions 100ms (by default) "large", containing all records sent during that time period, from the
         * offsets being committed.
         * <p>
         * Of no use, if not also producing messages (i.e. using a {@link ParallelStreamProcessor#pollAndProduce}
         * variation).
         * <p>
         * Note: Records being sent by different threads will all be in a single transaction, as PC shares a single
         * Producer instance. This could be seen as a performance overhead advantage, efficient resource use, in
         * exchange for a loss in transaction granularity.
         * <p>
         * The benefits of using this mode are:
         * <p>
         * a) All records produced from a given source offset will either all be visible, or none will be
         * ({@link org.apache.kafka.common.IsolationLevel#READ_COMMITTED}).
         * <p>
         * b) If any records making up a transaction have a terminal issue being produced, or the system crashes before
         * finishing sending all the records and committing, none will ever be visible and the system will eventually
         * retry them in new transactions - potentially with different combinations of records from the original.
         * <p>
         * c) A source offset, and it's produced records will be committed as an atomic set. Normally: either the record
         * producing could fail, or the committing of the source offset could fail, as they are separate individual
         * operations. When using Transactions, they are committed together - so if either operations fails, the
         * transaction will never get committed, and upon recovery, the system will retry the set again (and no
         * duplicates will be visible in the topic).
         * <p>
         * This {@code CommitMode} is the slowest of the options, but there will be no duplicates in Kafka caused by
         * producing a record multiple times if previous offset commits have failed or crashes have occurred (however
         * message replay may cause duplicates in external systems which is unavoidable - external systems must be
         * idempotent).
         * <p>
         * The default commit interval {@link AbstractParallelEoSStreamProcessor#KAFKA_DEFAULT_AUTO_COMMIT_FREQUENCY}
         * gets automatically reduced from the default of 5 seconds to 100ms (the same as Kafka Streams <a
         * href=https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html">commit.interval.ms</a>).
         * Reducing this configuration places higher load on the broker, but will reduce (but cannot eliminate) replay
         * upon failure. Note also that when using transactions in Kafka, consumption in {@code READ_COMMITTED} mode is
         * blocked up to the offset of the first STILL open transaction. Using a smaller commit frequency reduces this
         * minimum consumption latency - the faster transactions are closed, the faster the transaction content can be
         * read by {@code READ_COMMITTED} consumers. More information about this can be found on the Confluent blog
         * post:
         * <a href="https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/">Enabling Exactly-Once in Kafka
         * Streams</a>.
         * <p>
         * When producing multiple records (see {@link ParallelStreamProcessor#pollAndProduceMany}), all records must
         * have been produced successfully to the broker before the transaction will commit, after which all will be
         * visible together, or none.
         * <p>
         * Records produced while running in this mode, won't be seen by consumer running in
         * {@link ConsumerConfig#ISOLATION_LEVEL_CONFIG} {@link org.apache.kafka.common.IsolationLevel#READ_COMMITTED}
         * mode until the transaction is complete and all records are produced successfully. Records produced into a
         * transaction that gets aborted or timed out, will never be visible.
         * <p>
         * The system must prevent records from being produced to the brokers whose source consumer record offsets has
         * not been included in this transaction. Otherwise, the transactions would include produced records from
         * consumer offsets which would only be committed in the NEXT transaction, which would break the EoS guarantees.
         * To achieve this, first work processing and record producing is suspended (by acquiring the commit lock -
         * see{@link #commitLockAcquisitionTimeout}, as record processing requires the produce lock), then succeeded
         * consumer offsets are gathered, transaction commit is made, then when the transaction has finished, processing
         * resumes by releasing the commit lock. This periodically slows down record production during this phase, by
         * the time needed to commit the transaction.
         * <p>
         * This is all separate from using an IDEMPOTENT Producer, which can be used, along with the
         * {@link ParallelConsumerOptions#commitMode} {@link CommitMode#PERIODIC_CONSUMER_SYNC} or
         * {@link CommitMode#PERIODIC_CONSUMER_ASYNCHRONOUS}.
         * <p>
         * Failure:
         * <p>
         * Commit lock: If the system cannot acquire the commit lock in time, it will shut down for whatever reason, the
         * system will shut down (fail fast) - during the shutdown a final commit attempt will be made. The default
         * timeout for acquisition is very high though - see {@link #commitLockAcquisitionTimeout}. This can be caused
         * by the user processing function taking too long to complete.
         * <p>
         * Produce lock: If the system cannot acquire the produce lock in time, it will fail the record processing and
         * retry the record later. This can be caused by the controller taking too long to commit for some reason. See
         * {@link #produceLockAcquisitionTimeout}. If using {@link #allowEagerProcessingDuringTransactionCommit}, this
         * may cause side effect replay when the record is retried, otherwise there is no replay. See
         * {@link #allowEagerProcessingDuringTransactionCommit} for more details.
         *
         * @see ParallelConsumerOptions.ParallelConsumerOptionsBuilder#commitInterval
         */
        // end::transactionalJavadoc[]
        PERIODIC_TRANSACTIONAL_PRODUCER,

        /**
         * Periodically synchronous commits with the Consumer. Much faster than
         * {@link #PERIODIC_TRANSACTIONAL_PRODUCER}. Slower but potentially fewer duplicates than
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
     * Kafka's default auto commit interval - which is 5000ms.
     *
     * @see org.apache.kafka.clients.consumer.ConsumerConfig#AUTO_COMMIT_INTERVAL_MS_CONFIG
     * @see org.apache.kafka.clients.consumer.ConsumerConfig#CONFIG
     */
    public static final int KAFKA_DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000;

    public static final Duration DEFAULT_COMMIT_INTERVAL = ofMillis(KAFKA_DEFAULT_AUTO_COMMIT_INTERVAL_MS);

    /*
     * The same as Kafka Streams
     */
    public static final Duration DEFAULT_COMMIT_INTERVAL_FOR_TRANSACTIONS = ofMillis(100);

    /**
     * When using {@link CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER}, allows new records to be processed UP UNTIL the
     * result record SENDING ({@link Producer#send}) step, potentially while a transaction is being committed. Disabled
     * by default as to prevent replay side effects when records need to be retried in some scenarios.
     * <p>
     * Doesn't interfere with the transaction itself, just reduces side effects.
     * <p>
     * Recommended to leave this off to avoid side effect duplicates upon rebalances after a crash. Enabling could
     * improve performance as the produce lock will only be taken right before it's needed (optimistic locking) to
     * produce the result record, instead of pessimistically locking.
     */
    @Builder.Default
    private boolean allowEagerProcessingDuringTransactionCommit = false;

    /**
     * Time to allow for acquiring the commit lock. If record processing or producing takes a long time, you may need to
     * increase this. If this fails, the system will shut down (fail fast) and attempt to commit once more.
     */
    @Builder.Default
    private Duration commitLockAcquisitionTimeout = Duration.ofMinutes(5);

    /**
     * Time to allow for acquiring the produce lock. If transaction committing a long time, you may need to increase
     * this. If this fails, the record will be returned to the processing queue for later retry.
     */
    @Builder.Default
    private Duration produceLockAcquisitionTimeout = Duration.ofMinutes(1);

    /**
     * Time between commits. Using a higher frequency (a lower value) will put more load on the brokers.
     */
    @Builder.Default
    private Duration commitInterval = DEFAULT_COMMIT_INTERVAL;

    /**
     * @deprecated only settable during {@code deprecation phase} - use
     *         {@link ParallelConsumerOptions.ParallelConsumerOptionsBuilder#commitInterval}} instead.
     */
    // todo delete in next major version
    @Deprecated
    public void setCommitInterval(Duration commitInterval) {
        this.commitInterval = commitInterval;
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

    public static final Duration DEFAULT_STATIC_RETRY_DELAY = Duration.ofSeconds(1);

    /**
     * When a message fails, how long the system should wait before trying that message again. Note that this will not
     * be exact, and is just a target.
     *
     * @deprecated will be renamed to static retry delay
     */
    @Deprecated
    @Builder.Default
    private final Duration defaultMessageRetryDelay = DEFAULT_STATIC_RETRY_DELAY;

    /**
     * When present, use this to generate a dynamic retry delay, instead of a static one with
     * {@link #getDefaultMessageRetryDelay()}.
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
     The minimum number of batch to pass into the user functions.
     <p>
     If the available number of messages is less than {@code minBatchSize}, they will not be processed until more messages
     arrive or {@code minBatchTimeoutInMillis} has passed.
     <p>
     Note that this parameter only takes effect if it is greater than 1 and the {@code minBatchTimeoutInMillis} parameter
     is greater than 0.
     @see ParallelConsumerOptions#getMinBatchTimeoutInMillis()
     */
    @Builder.Default
    private final Integer minBatchSize = 1;

    /**
     The minimum time in milliseconds to wait before passing a batch of messages to the user functions, even if the
     {@code minBatchSize} threshold has not been reached.
     <p>
     Note that this parameter only takes effect if it is greater than 0 and the {@code minBatchSize} parameter is greater
     than 1.
     @see ParallelConsumerOptions#getMinBatchSize()
     */
    @Builder.Default
    private final Integer minBatchTimeoutInMillis = 0;

    /**
     * Configure the amount of delay a record experiences, before a warning is logged.
     */
    @Builder.Default
    private final Duration thresholdForTimeSpendInQueueWarning = Duration.ofSeconds(10);

    public boolean isUsingBatching() {
        return getBatchSize() > 1;
    }

    public boolean isEnforceMinBatch() { return getMinBatchSize() > 1 && getMinBatchTimeoutInMillis() > 0; }

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

        transactionsValidation();
        validateMinBatchParameters();
    }

    private void validateMinBatchParameters() {
        if (isEnforceMinBatch()){
            if (minBatchSize > batchSize)
                throw new IllegalArgumentException(
                        msg("minBatchSize cannot be bigger than batchSize: {} > {}",
                    minBatchSize,
                    batchSize));
        }
        if (minBatchTimeoutInMillis < 0){
            throw new IllegalArgumentException(
                    msg("minBatchTimeoutInMillis should be non negative: {}",
                            minBatchTimeoutInMillis
                    ));
        }
    }

    private void transactionsValidation() {
        boolean commitInternalHasNotBeenSet = getCommitInterval() == DEFAULT_COMMIT_INTERVAL;

        if (isUsingTransactionCommitMode()) {
            if (producer == null) {
                throw new IllegalArgumentException(msg("Cannot set {} to Transaction Producer mode ({}) without supplying a Producer instance",
                        Fields.commitMode,
                        commitMode));
            }

            // update commit frequency
            if (commitInternalHasNotBeenSet) {
                this.commitInterval = DEFAULT_COMMIT_INTERVAL_FOR_TRANSACTIONS;
            }
        }

        // inverse
        if (!isUsingTransactionCommitMode()) {
            if (isAllowEagerProcessingDuringTransactionCommit()) {
                throw new IllegalArgumentException(msg("Cannot set {} (eager record processing) when not using transactional commit mode ({}={}).",
                        Fields.allowEagerProcessingDuringTransactionCommit,
                        Fields.commitMode,
                        commitMode));
            }
        }
    }

    /**
     * @deprecated use {@link #isUsingTransactionCommitMode()}
     */
    @Deprecated
    public boolean isUsingTransactionalProducer() {
        return isUsingTransactionCommitMode();
    }

    /**
     * @see CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER
     */
    public boolean isUsingTransactionCommitMode() {
        return commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER);
    }

    public boolean isProducerSupplied() {
        return getProducer() != null;
    }
}
