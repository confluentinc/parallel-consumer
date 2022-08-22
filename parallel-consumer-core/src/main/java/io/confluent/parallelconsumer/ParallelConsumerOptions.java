package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static java.time.Duration.ofMillis;

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
     * todo docs
     */
    @Getter
    @Setter
    private PCModule<K, V> module;

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
         * Unlike Kafka Streams, the records being sent by different threads will all be in a single transaction, as PC
         * shares a single Producer instance. This could be seen as an performance overhead advantage, efficient
         * resource use, for a loss in granularity.
         * <p>
         * The benefits of using this mode are:
         * <p>
         * a) All records produced from a given source offset will either all be visible, or none will be
         * ({@link org.apache.kafka.common.IsolationLevel#READ_COMMITTED}).
         * <p>
         * b) If any records making up a transaction have a terminal issue being produced, or the system crashes before
         * finishing sending all the records and committing, none will ever be visible and the system will retry the
         * whole set in a new transaction.
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
         * Note that the system can potentially cause very large transactions (for transaction standards). The default
         * commit interval {@link AbstractParallelEoSStreamProcessor#KAFKA_DEFAULT_AUTO_COMMIT_FREQUENCY} gets
         * automatically reduced from the default of 5 seconds to 100ms (the same as Kafka Streams <a
         * href=https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html">commit.interval.ms</a>).
         * Reducing this configuration places higher loads on the broker, but will reduce (but cannot eliminate) replay
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
         * transactions that gets aborted or timed out, will never be visible.
         * <p>
         * The system must prevent records from being produced to the brokers whose source consumer record offsets has
         * not been included in this transaction. Otherwise, the transactions would include produced records from
         * consumer offsets which would only be committed in the NEXT transaction, which wouldn't make sense. To achieve
         * this, after succeeded consumer offsets are gathered, work processing and record producing is suspended, until
         * the transaction has finished committing. This periodically slows down record production during this phase, by
         * the time needed to commit the transaction.
         * <p>
         * This is all separate from using an IDEMPOTENT Producer, which can be used, along with
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
     * Kafka's default auto commit frequency - which is 5000ms.
     *
     * @see org.apache.kafka.clients.consumer.ConsumerConfig#AUTO_COMMIT_INTERVAL_MS_CONFIG
     * @see org.apache.kafka.clients.consumer.ConsumerConfig#CONFIG
     */
    public static final int KAFKA_DEFAULT_AUTO_COMMIT_FREQUENCY_MS = 5000;

    public static final Duration DEFAULT_TIME_BETWEEN_COMMITS = ofMillis(KAFKA_DEFAULT_AUTO_COMMIT_FREQUENCY_MS);

    /*
     * The same as Kafka Streams
     */
    public static final Duration DEFAULT_TIME_BETWEEN_COMMITS_FOR_TRANSACTIONS = ofMillis(100);

    /**
     * Allow new records to be processed while a transaction is being processed. Default disabled.
     * <p>
     * Recommended to leave this off to avoid side effect duplicates upon rebalance. Enabling could improve performance
     * as the produce lock will only be taken right before it's needed to produce the result record.
     */
    @Builder.Default
    private boolean allowEagerProcessingDuringTransactionCommit = false;

    /**
     * Time between commits. Using a higher frequency (a lower value) will put more load on the brokers.
     */
    @Builder.Default
    private Duration timeBetweenCommits = DEFAULT_TIME_BETWEEN_COMMITS;

    /**
     * @deprecated only settable during {@code deprecation phase} - use
     *         {@link  ParallelConsumerOptions.ParallelConsumerOptionsBuilder#timeBetweenCommits}} instead.
     */
    // todo delete in next major version
    @Deprecated
    public void setTimeBetweenCommits(Duration timeBetweenCommits) {
        this.timeBetweenCommits = timeBetweenCommits;
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

        transactionsValidation();

        //
        WorkContainer.setDefaultRetryDelay(getDefaultMessageRetryDelay());
    }

    private void transactionsValidation() {
        if (isUsingTransactionalProducer()) {
            if (producer == null) {
                throw new IllegalArgumentException(msg("Wanting to use Transaction Producer mode ({}) without supplying a Producer instance",
                        commitMode));
            }

            // update commit frequency
            boolean commitFrequencyHasntBeenSet = getTimeBetweenCommits() == DEFAULT_TIME_BETWEEN_COMMITS;
            if (commitFrequencyHasntBeenSet) {
                this.timeBetweenCommits = DEFAULT_TIME_BETWEEN_COMMITS_FOR_TRANSACTIONS;
            }
        }
    }

    @Deprecated
    public boolean isUsingTransactionalProducer() {
        return isUsingTransactionCommitMode();
    }

    public boolean isUsingTransactionCommitMode() {
        return commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER);
    }

    public boolean isProducerSupplied() {
        return getProducer() != null;
    }
}
