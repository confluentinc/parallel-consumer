package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.*;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.TimeoutException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.confluent.csid.utils.StringUtils.msg;

/**
 * Sub system for interacting with the Producer and managing transactions (and thus offset committing through the
 * Producer).
 */
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
public class ProducerManager<K, V> extends AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    @Getter
    protected final ProducerWrap<K, V> producerWrap;

    private final ParallelConsumerOptions<K, V> options;

    /**
     * The {@link KafkaProducer} isn't actually completely thread safe, at least when using it transitionally. We must
     * be careful not to send messages to the producer, while we are committing a transaction - "Cannot call send in
     * state COMMITTING_TRANSACTION".
     * <p>
     * We also need to use this as a synchronisation barrier on transactions - so that when we start a commit cycle, we
     * first block any further records from being sent, then drain ourselves to get all sent records ack'd, and then
     * commit the tx during the synchronisation barrier, then unlock the barrier.
     * <p>
     * This could be implemented more simply, using the new micro Actor system, by sending {@link ProducerRecord}s as
     * actor messages, and having the controller process the {@link ProducerManager}s actor queue (send the queued up
     * records). However, given our implementation, that would have the side effect of all producer record sending being
     * done by the controller thread. Now as the Producer is thread safe - it uses the {@link RecordAccumulator}
     * effectively as it's Actor bus, and all network communication, amongst other things, are done through a separate
     * thread. However, before sending records to the accumulator, some non-trivial work is done while still in the
     * multithreading context - most particularly (because it's probably the slowest part) is the serialisation of the
     * payload. By moving to the new micro Actor framework, that serialisation would then be done in the controller.
     * Give the existing shared state system using the {@link ReentrantReadWriteLock} works really well, and so sending
     * work is done by worker threads, I'm hesitant to give up the performance over simplification in this case.
     */
    @Getter
    private ReentrantReadWriteLock producerTransactionLock;

    public ProducerManager(ProducerWrap<K, V> newProducer,
                           ConsumerManager<K, V> newConsumer,
                           WorkManager<K, V> wm,
                           ParallelConsumerOptions<K, V> options) {
        super(newConsumer, wm);
        this.producerWrap = newProducer;
        this.options = options;

        initProducer();
    }

    private void initProducer() {
        producerTransactionLock = new ReentrantReadWriteLock(true);

        if (options.isUsingTransactionalProducer()) {
            if (!producerWrap.isConfiguredForTransactions()) {
                throw new IllegalArgumentException("Using transactional option, yet Producer doesn't have a transaction ID - Producer needs a transaction id");
            }
            try {
                log.debug("Initialising producer transaction session...");
                producerWrap.initTransactions();
            } catch (KafkaException e) {
                log.error("Make sure your producer is setup for transactions - specifically make sure it's {} is set.", ProducerConfig.TRANSACTIONAL_ID_CONFIG, e);
                throw e;
            }
        } else {
            if (producerWrap.isConfiguredForTransactions()) {
                throw new IllegalArgumentException("Using non-transactional producer option, but Producer has a transaction ID - "
                        + "the Producer must not have a transaction ID for this option. This is because having such an ID forces the "
                        + "Producer into transactional mode - i.e. you cannot use it without using transactions.");
            }
        }
    }

    /**
     * Produce a message back to the broker.
     * <p>
     * Implementation uses the blocking API, by blocking on produce ack results (in batches when the flatMap version of
     * producing a list of records is used). Performance upgrade in later versions (#356). This is of course not an
     * issue for the more common use case of PC where messages aren't produced
     * ({@link ParallelEoSStreamProcessor#poll}), and the {@code produce ack block} is still multi-threaded after all.
     * <p>
     * May block while a transaction is in progress - see
     * {@link ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER}.
     *
     * @see ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER
     * @see ParallelStreamProcessor#pollAndProduceMany
     */
    public List<ParallelConsumer.Tuple<ProducerRecord<K, V>, Future<RecordMetadata>>> produceMessages(List<ProducerRecord<K, V>> outMsgs) {
        ensureProduceStarted();
        lazyMaybeBeginTransaction();

        // only needed if not using tx
        Callback callback = (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                log.error("Error producing result message", exception);
                throw new InternalRuntimeError("Error producing result message", exception);
            }
        };

        List<ParallelConsumer.Tuple<ProducerRecord<K, V>, Future<RecordMetadata>>> futures = new ArrayList<>(outMsgs.size());
        for (ProducerRecord<K, V> rec : outMsgs) {
            log.trace("Producing {}", rec);
            var future = producerWrap.send(rec, callback);
            futures.add(ParallelConsumer.Tuple.pairOf(rec, future));
        }
        return futures;
    }

    /**
     * Optimistic locking for synchronising on the producer to ensure single writer for transaction state. The other
     * methods that manipulate the transaction must be single writer - i.e. from the controller thread actually doing
     * the commit.
     * <p>
     * Thread safe.
     */
    private void lazyMaybeBeginTransaction() {
        if (options.isUsingTransactionCommitMode()) {
            boolean txNotBegunAlready = !producerWrap.isTransactionOpen();
            if (txNotBegunAlready) {
                syncBeginTransaction();
            }
        }
    }

    /**
     * Pessimistic lock (synchronized method) on beginning a transaction
     * <p>
     * Thread safe.
     */
    private synchronized void syncBeginTransaction() {
        boolean txNotBegunAlready = !producerWrap.isTransactionOpen();
        if (txNotBegunAlready) {
            beginTransaction();
        }
    }

    protected void releaseProduceLock(ProducingLock lock) {
        lock.unlock();
    }

    protected ProducingLock acquireProduceLock(PollContextInternal<K, V> context) throws java.util.concurrent.TimeoutException {
        ReentrantReadWriteLock.ReadLock readLock = producerTransactionLock.readLock();
        Duration produceLockTimeout = options.getProduceLockAcquisitionTimeout();
        log.debug("Acquiring produce lock (timeout: {})...", produceLockTimeout);
        boolean lockAcquired = false;
        try {
            lockAcquired = readLock.tryLock(produceLockTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new InternalRuntimeError("Interrupted while waiting to get produce lock (timeout was set to {})", e, produceLockTimeout);
        }

        if (lockAcquired) {
            log.debug("Produce lock acquired (context: {}).", context.getOffsets());
        } else {
            throw new java.util.concurrent.TimeoutException(msg("Timeout while waiting to get produce lock (was set to {}). " +
                    "Commit taking too long? Try increasing the produce lock timeout.", produceLockTimeout));
        }

        log.trace("Produce lock acquired.");
        return new ProducingLock(context, readLock);
    }

    /**
     * First lock, so no other records can be sent. Then wait for the producer to get all its {@code acks} complete by
     * calling {@link Producer#flush()}.
     */
    @Override
    protected void preAcquireWork() throws java.util.concurrent.TimeoutException, InterruptedException {
        acquireCommitLock();
        drain();
    }


    /**
     * Wait for all in flight records to be ack'd before continuing, so they are all in the tx.
     */
    private void drain() {
        producerWrap.flush();
    }

    /**
     * Only release lock when commit successful
     */
    @Override
    protected void postCommit() {
        if (producerTransactionLock.getWriteHoldCount() > 1) // sanity
            throw new ConcurrentModificationException("Lock held too many times, won't be released problem and will cause deadlock");

        releaseCommitLock();
    }

    /**
     * @see InvalidProducerEpochException
     * @see KafkaProducer#commitTransaction()
     */
    @Override
    protected void commitOffsets(@NonNull Map<TopicPartition, OffsetAndMetadata> offsetsToSend, @NonNull ConsumerGroupMetadata groupMetadata) {
        log.debug("Transactional offset commit starting");
        if (!options.isUsingTransactionalProducer()) {
            throw new IllegalStateException("Bug: cannot use if not using transactional producer");
        }

        // producer commit lock should already be acquired at this point, before work was retrieved to commit,
        // so that more messages don't sneak into this tx block - the consumer records of which won't yet be
        // in this offset collection
        ensureCommitLockHeld();

        //
        lazyMaybeBeginTransaction(); // if not using a produce flow, a tx will need to be started here (as no records are being produced)
        producerWrap.sendOffsetsToTransaction(offsetsToSend, groupMetadata);

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
                if (producerWrap.isMockProducer()) {
                    // see bug https://issues.apache.org/jira/browse/KAFKA-10382
                    // KAFKA-10382 - MockProducer is not ThreadSafe, ideally it should be as the implementation it mocks is
                    synchronized (producerWrap) {
                        commitTransaction();
                    }
                } else {
                    // TODO talk about alternatives to this brute force approach for retrying committing transactions
                    boolean retrying = retryCount > 0;
                    if (retrying) {
                        if (producerWrap.isTransactionCompleting()) {
                            // try wait again
                            commitTransaction();
                        }
                        boolean transactionModeIsREADY = lastErrorSavedForRethrow == null || !lastErrorSavedForRethrow.getMessage().contains("Invalid transition attempted from state READY to state COMMITTING_TRANSACTION");
                        if (transactionModeIsREADY) {
                            // try again
                            log.error("Transaction was already in READY state - tx completed between interrupt and retry");
                        }
                    } else {
                        // happy path
                        commitTransaction();
                    }
                }

                committed = true;
                if (retryCount > 0) {
                    log.warn("Commit success, but took {} tries.", retryCount);
                }
            }
            /*
            Producer#begin does not throw any retriable exceptions

            Producer#commit throws the following exceptions:

             // terminal general
             AuthorizationException – fatal error indicating that the configured transactional.id is not authorized. See the exception for more details
             KafkaException – if the producer has encountered a previous fatal or abortable error, or for any other unexpected error

             // terminal tx
             IllegalStateException – if no transactional.id has been configured or no transaction has been started
             UnsupportedVersionException – fatal error indicating the broker does not support transactions (i.e. if its version is lower than 0.11.0.0)
             ProducerFencedException – fatal error indicating another producer with the same transactional.id is active
             InvalidProducerEpochException – if the producer has attempted to produce with an old epoch to the partition leader. See the exception for more details
             - as per - InvalidProducerEpochException javadoc the, the tx should be aborted and the Producer initialised again, so to fail
               this we will just fail fast and have to be restarted

             // retriable tx
             TimeoutException – if the time taken for committing the transaction has surpassed max.block.ms.
             InterruptException – if the thread is interrupted while blocked

             Only catch and retry the retriable ones, others fail fast the control thread
             todo verify

             */ catch (TimeoutException | InterruptException e) {
                log.warn("Commit exception, will retry, have tried {} times (see KafkaProducer#commit)", retryCount, e);
                lastErrorSavedForRethrow = e;
                retryCount++;
            }
        }
    }

    private void commitTransaction() {
        producerWrap.commitTransaction();
    }

    private void beginTransaction() {
        /*
         // terminal general
         AuthorizationException – fatal error indicating that the configured transactional.id is not authorized. See the exception for more details
         KafkaException – if the producer has encountered a previous fatal error or for any other unexpected error

         // terminal tx
         IllegalStateException – if no transactional.id has been configured or if initTransactions() has not yet been invoked
         UnsupportedVersionException – fatal error indicating the broker does not support transactions (i.e. if its version is lower than 0.11.0.0)
         ProducerFencedException – if another producer with the same transactional.id is active
         InvalidProducerEpochException – if the producer has attempted to produce with an old epoch to the partition leader. See the exception for more details

         // retriable tx
         none
         */
        producerWrap.beginTransaction();
    }

    /**
     * Assumes the system is drained at this point, or draining is not desired.
     */
    public void close(Duration timeout) {
        log.debug("Closing producer, assuming no more in flight...");
        if (options.isUsingTransactionalProducer() && !producerWrap.isTransactionReady()) {
            try {
                acquireCommitLock();
            } catch (java.util.concurrent.TimeoutException | InterruptedException e) {
                log.error("Exception acquiring commit lock, will try to abort anyway", e);
            }
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
        producerWrap.close(timeout);
    }

    private void abortTransaction() {
        producerWrap.abortTransaction();
    }

    private void acquireCommitLock() throws java.util.concurrent.TimeoutException, InterruptedException {
        if (producerTransactionLock.isWriteLocked() && producerTransactionLock.isWriteLockedByCurrentThread()) {
            log.debug("Lock already held, returning with-out reentering to avoid write lock layers...");
            return;
        }

        ReentrantReadWriteLock.WriteLock writeLock = producerTransactionLock.writeLock();
        if (producerTransactionLock.isWriteLocked() && !producerTransactionLock.isWriteLockedByCurrentThread()) {
            throw new ConcurrentModificationException(this.getClass().getSimpleName() + " is not safe for multi-threaded access");
        }

        // acquire lock the commit lock
        var commitLockTimeout = options.getCommitLockAcquisitionTimeout();
        log.debug("Acquiring commit lock (timeout: {})...", commitLockTimeout);
        boolean gotLock = writeLock.tryLock(commitLockTimeout.toMillis(), TimeUnit.MILLISECONDS);

        if (gotLock) {
            log.debug("Commit lock acquired.");
        } else {
            var msg = msg("Timeout getting commit lock (which was set to {}). Slow processing or too many records being ack'd? " +
                            "Try increasing the commit lock timeout ({}), or reduce your record processing time.",
                    commitLockTimeout,
                    ParallelConsumerOptions.Fields.commitLockAcquisitionTimeout
            );
            throw new java.util.concurrent.TimeoutException(msg);
        }
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

    /**
     * Must call before sending records - acquires the lock on sending records, which blocks committing transactions)
     */
    public ProducingLock beginProducing(PollContextInternal<K, V> context) throws java.util.concurrent.TimeoutException {
        return acquireProduceLock(context);
    }

    /**
     * Must call after finishing sending records - unlocks the produce lock to potentially unblock transaction
     * committing.
     */
    public void finishProducing(@NonNull ProducingLock produceLock) {
        ensureProduceStarted();
        releaseProduceLock(produceLock);
    }

    /**
     * Sanity check to make sure the produce lock is held.
     */
    private void ensureProduceStarted() {
        if (options.isUsingTransactionCommitMode() && producerTransactionLock.getReadHoldCount() < 1) {
            throw new InternalRuntimeError("Need to call #beginProducing first");
        }
    }

    /**
     * Readability wrapper on the {@link ReentrantReadWriteLock.ReadLock}s of our {@link #producerTransactionLock}.
     */
    @RequiredArgsConstructor
    public class ProducingLock {

        private final PollContextInternal<K, V> context;
        private final ReentrantReadWriteLock.ReadLock produceLock;

        /**
         * Unlocks the produce lock
         */
        protected void unlock() {
            produceLock.unlock();
            log.debug("Unlocking produce locked (context: {}).", context.getOffsets());
        }
    }
}
