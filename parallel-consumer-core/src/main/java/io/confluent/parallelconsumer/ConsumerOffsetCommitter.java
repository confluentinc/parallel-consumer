package io.confluent.parallelconsumer;

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.CONSUMER_SYNC;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.TRANSACTIONAL_PRODUCER;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Committer that uses the Kafka Consumer to commit either synchronously or asynchronously
 *
 * @see CommitMode
 */
@Slf4j
public class ConsumerOffsetCommitter<K, V> extends AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    private final CommitMode commitMode;

    /**
     * Used to synchronise threads on changing {@link #commitCount}, {@link #commitPerformed} and constructing the
     * {@link Condition} to wait on, that's all.
     */
    private final ReentrantLock commitLock = new ReentrantLock(true);

    /**
     * Used to signal to waiting threads, that their commit has been performed when requested.
     *
     * @see #commitPerformed
     * @see #commitAndWaitForCondition()
     */
    private Condition commitPerformed = commitLock.newCondition();

    /**
     * The number of commits made. Use as a logical clock to sanity check expectations and synchronisation when commits
     * are requested versus performed.
     */
    private final AtomicLong commitCount = new AtomicLong(0);

    /**
     * Set to true when a thread requests a commit to be performed, by the controling thread.
     */
    private final AtomicBoolean commitRequested = new AtomicBoolean(false);

    private Optional<Thread> owningThread = Optional.empty();

    public ConsumerOffsetCommitter(final ConsumerManager<K, V> newConsumer, final WorkManager<K, V> newWorkManager, final ParallelConsumerOptions options) {
        super(newConsumer, newWorkManager);
        commitMode = options.getCommitMode();
        if (commitMode.equals(TRANSACTIONAL_PRODUCER)) {
            throw new IllegalArgumentException("Cannot use " + commitMode + " when using " + this.getClass().getSimpleName());
        }
    }

    // todo abstraction leak - find another way
    private boolean direct = false;

    /**
     * Might block if using {@link CommitMode#CONSUMER_SYNC}
     *
     * @see CommitMode
     */
    void commit() {
        if (isOwner()) {
            // if owning thread is asking, then perform the commit directly (this is the thread that controls the consumer)
            // this can happen when the system is closing, using Consumer commit mode, and partitions are revoked and we want to commit
            direct = true;
            retrieveOffsetsAndCommit();
        } else if (isSync()) {
            log.debug("Sync commit");
            commitAndWaitForCondition();
            log.debug("Finished waiting");
        } else {
            // async
            // we just request the commit and hope
            log.debug("Async commit to be requested");
            requestCommitInternal();
        }
    }

    @Override
    protected void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata) {
        if (offsetsToSend.isEmpty()) {
            log.trace("Nothing to commit");
            return;
        }
        switch (commitMode) {
            case CONSUMER_SYNC -> {
                log.debug("Committing offsets Sync");
                consumerMgr.commitSync(offsetsToSend);
            }
            case CONSUMER_ASYNCHRONOUS -> {
                //
                log.debug("Committing offsets Async");
                consumerMgr.commitAsync(offsetsToSend, (offsets, exception) -> {
                    if (exception != null) {
                        log.error("Error committing offsets", exception);
                        // todo keep work in limbo until async response is received?
                    }
                });
            }
            default -> {
                throw new IllegalArgumentException("Cannot use " + commitMode + " when using " + this.getClass().getSimpleName());
            }
        }
    }

    /**
     * @see #commit()
     */
    @Override
    protected void postCommit() {
        // only signal if we are in sync mode, and current thread isn't the owner (if we are owner, then we are committing directly)
        if (!direct && commitMode.equals(CONSUMER_SYNC))
            signalCommitPerformed();
    }

    private boolean isOwner() {
        return Thread.currentThread().equals(owningThread.orElse(null));
    }

    private void signalCommitPerformed() {
        log.debug("Starting Signaling commit finished");
        if (!commitLock.isHeldByCurrentThread())
            throw new IllegalStateException("Lock already held");
        commitLock.lock();
        try {
            commitCount.incrementAndGet();
            log.debug("Signaling commit finished");
            commitPerformed.signalAll();
            log.debug("Finished Signaling commit finished");
        } finally {
            commitLock.unlock();
        }
    }

    private void commitAndWaitForCondition() {
        commitLock.lock();

        try {
            this.commitPerformed = commitLock.newCondition();
            long currentCount = commitCount.get();
            requestCommitInternal();
            while (currentCount == commitCount.get()) {
                if (currentCount == commitCount.get()) {
                    log.debug("Requesting commit again");
                    requestCommitInternal();
                } else {
                    commitRequested.set(false);
                }
                try {
                    log.debug("Waiting on commit");
                    commitPerformed.await();
                } catch (InterruptedException e) {
                    log.debug("Interrupted waiting for commit condition", e);
                }
            }
            log.debug("Signaled");
        } finally {
            commitLock.unlock();
        }
    }

    private void requestCommitInternal() {
        commitLock.lock();
        try {
            commitRequested.set(true);
            consumerMgr.onCommitRequested();
            consumerMgr.wakeup();
        } finally {
            commitLock.unlock();
        }

    }

    void maybeDoCommit() {
        commitLock.lock();
        try {
            if (commitRequested.get()) {
                log.debug("Commit requested, performing...");
                retrieveOffsetsAndCommit();
            }
        } finally {
            commitLock.unlock();
        }
    }

    public boolean isSync() {
        return commitMode.equals(CONSUMER_SYNC);
    }

    public void claim() {
        owningThread = Optional.of(Thread.currentThread());
    }
}
