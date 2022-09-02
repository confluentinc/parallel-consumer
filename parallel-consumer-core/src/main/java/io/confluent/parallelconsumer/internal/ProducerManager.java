package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.confluent.csid.utils.StringUtils.msg;

/**
 * Sub system for interacting with the Producer and managing transactions (and thus offset committing through the
 * Producer).
 */
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
public class ProducerManager<K, V> extends AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    protected final ProducerWrapper<K, V> producer;

    private final ParallelConsumerOptions<K, V> options;

    /**
     * The {@link KafkaProducer} isn't actually completely thread safe, at least when using it transactionally. We must
     * be careful not to send messages to the producer, while we are committing a transaction - "Cannot call send in
     * state COMMITTING_TRANSACTION".
     */
    private ReentrantReadWriteLock producerTransactionLock;

    public ProducerManager(ProducerWrapper<K, V> newProducer,
                           ConsumerManager<K, V> newConsumer,
                           WorkManager<K, V> wm,
                           ParallelConsumerOptions<K, V> options) {
        super(newConsumer, wm);
        this.producer = newProducer;
        this.options = options;

        initProducer();
    }

    private void initProducer() {
        producerTransactionLock = new ReentrantReadWriteLock(true);

        if (options.isUsingTransactionalProducer()) {
            if (!producer.isConfiguredForTransactions()) {
                throw new IllegalArgumentException("Using transactional option, yet Producer doesn't have a transaction ID - Producer needs a transaction id");
            }
            try {
                log.debug("Initialising producer transaction session...");
                producer.initTransactions();
                producer.beginTransaction();
            } catch (KafkaException e) {
                log.error("Make sure your producer is setup for transactions - specifically make sure it's {} is set.", ProducerConfig.TRANSACTIONAL_ID_CONFIG, e);
                throw e;
            }
        } else {
            if (producer.isConfiguredForTransactions()) {
                throw new IllegalArgumentException("Using non-transactional producer option, but Producer has a transaction ID - "
                        + "the Producer must not have a transaction ID for this option. This is because having such an ID forces the "
                        + "Producer into transactional mode - i.e. you cannot use it without using transactions.");
            }
        }
    }

    /**
     * Produce a message back to the broker.
     * <p>
     * Implementation uses the blocking API, performance upgrade in later versions, is not an issue for the more common
     * use case where messages aren't produced.
     *
     * @see ParallelConsumer#poll
     * @see ParallelStreamProcessor#pollAndProduceMany
     */
    public List<ParallelConsumer.Tuple<ProducerRecord<K, V>, Future<RecordMetadata>>> produceMessages(List<ProducerRecord<K, V>> outMsgs) {
        // only needed if not using tx
        Callback callback = (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                log.error("Error producing result message", exception);
                throw new RuntimeException("Error producing result message", exception);
            }
        };

        ReentrantReadWriteLock.ReadLock readLock = producerTransactionLock.readLock();
        readLock.lock();
        List<ParallelConsumer.Tuple<ProducerRecord<K, V>, Future<RecordMetadata>>> futures = new ArrayList<>(outMsgs.size());
        try {
            for (ProducerRecord<K, V> rec : outMsgs) {
                log.trace("Producing {}", rec);
                var future = producer.send(rec, callback);
                futures.add(ParallelConsumer.Tuple.pairOf(rec, future));
            }
        } finally {
            readLock.unlock();
        }

        return futures;
    }

    @Override
    protected void preAcquireWork() {
        acquireCommitLock();
    }

    @Override
    protected void postCommit() {
        // only release lock when commit successful
        if (producerTransactionLock.getWriteHoldCount() > 1) // sanity
            throw new ConcurrentModificationException("Lock held too many times, won't be released problem and will cause deadlock");

        releaseCommitLock();
    }

    @Override
    protected void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata) {
        log.debug("Transactional offset commit starting");
        if (!options.isUsingTransactionalProducer()) {
            throw new IllegalStateException("Bug: cannot use if not using transactional producer");
        }

        producer.sendOffsetsToTransaction(offsetsToSend, groupMetadata);
        // see {@link KafkaProducer#commit} this can be interrupted and is safe to retry
        boolean committed = false;
        int retryCount = 0;
        int arbitrarilyChosenLimitForArbitraryErrorSituation = 200;
        Exception lastErrorSavedForRethrow = null;
        while (!committed) {
            if (retryCount > arbitrarilyChosenLimitForArbitraryErrorSituation) {
                String msg = msg("Retired too many times ({} > limit of {}), giving up. See error above.", retryCount, arbitrarilyChosenLimitForArbitraryErrorSituation);
                log.error(msg, lastErrorSavedForRethrow);
                throw new InternalRuntimeError(msg, lastErrorSavedForRethrow);
            }
            try {
                if (producer.isMockProducer()) {
                    // see bug https://issues.apache.org/jira/browse/KAFKA-10382
                    // KAFKA-10382 - MockProducer is not ThreadSafe, ideally it should be as the implementation it mocks is
                    synchronized (producer) {
                        producer.commitTransaction();
                        producer.beginTransaction();
                    }
                } else {

                    // producer commit lock should already be acquired at this point, before work was retrieved to commit,
                    // so that more messages don't sneak into this tx block - the consumer records of which won't yet be
                    // in this offset collection
                    ensureCommitLockHeld();

                    // TODO talk about alternatives to this brute force approach for retrying committing transactions
                    boolean retrying = retryCount > 0;
                    if (retrying) {
                        if (producer.isTransactionCompleting()) {
                            // try wait again
                            commitTransaction();
                        }
                        if (producer.isTransactionReady()) {
                            // tx has completed since we last tried, start a new one
                            producer.beginTransaction();
                        }
                        boolean ready = lastErrorSavedForRethrow == null || !lastErrorSavedForRethrow.getMessage().contains("Invalid transition attempted from state READY to state COMMITTING_TRANSACTION");
                        if (ready) {
                            // try again
                            log.error("Transaction was already in READY state - tx completed between interrupt and retry");
                        }
                    } else {
                        // happy path
                        commitTransaction();
                        producer.beginTransaction();
                    }
                }

                committed = true;
                if (retryCount > 0) {
                    log.warn("Commit success, but took {} tries.", retryCount);
                }
            } catch (Exception e) {
                log.warn("Commit exception, will retry, have tried {} times (see KafkaProducer#commit)", retryCount, e);
                lastErrorSavedForRethrow = e;
                retryCount++;
            }
        }
    }

    private void commitTransaction() {
        producer.commitTransaction();
    }

    /**
     * Assumes the system is drained at this point, or draining is not desired.
     */
    public void close(Duration timeout) {
        log.debug("Closing producer, assuming no more in flight...");
        if (options.isUsingTransactionalProducer() && !producer.isTransactionReady()) {
            acquireCommitLock();
            try {
                // close started after tx began, but before work was done, otherwise a tx wouldn't have been started
                abortTransaction();
            } finally {
                releaseCommitLock();
            }
        }
        closeProducer(timeout);
    }

    private void closeProducer(Duration timeout) {
        producer.close(timeout);
    }

    private void abortTransaction() {
        producer.abortTransaction();
    }

    private void acquireCommitLock() {
        if (producerTransactionLock.getWriteHoldCount() > 0)
            throw new ConcurrentModificationException("Lock already held");
        ReentrantReadWriteLock.WriteLock writeLock = producerTransactionLock.writeLock();
        if (producerTransactionLock.isWriteLocked() && !producerTransactionLock.isWriteLockedByCurrentThread()) {
            throw new ConcurrentModificationException(this.getClass().getSimpleName() + " is not safe for multi-threaded access");
        }
        writeLock.lock();
    }

    private void releaseCommitLock() {
        log.debug("Releasing commit lock...");
        ReentrantReadWriteLock.WriteLock writeLock = producerTransactionLock.writeLock();
        if (!producerTransactionLock.isWriteLockedByCurrentThread())
            throw new IllegalStateException("Not held be me");
        writeLock.unlock();
        log.debug("Commit lock released.");
    }

    private void ensureCommitLockHeld() {
        if (!producerTransactionLock.isWriteLockedByCurrentThread())
            throw new IllegalStateException("Expected commit lock to be held");
    }

    /**
     * @return true if the commit lock has been acquired by any thread.
     */
    public boolean isTransactionCommittingInProgress() {
        return producerTransactionLock.isWriteLocked();
    }
}
