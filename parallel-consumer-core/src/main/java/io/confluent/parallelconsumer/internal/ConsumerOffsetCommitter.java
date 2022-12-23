package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.actors.ActorImpl;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;

/**
 * Committer that uses the Kafka Consumer to commit either synchronously or asynchronously
 *
 * @see CommitMode
 */
@Slf4j
public class ConsumerOffsetCommitter<K, V> extends AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    private final CommitMode commitMode;

    private final Duration commitTimeout;

    // todo code smell - should be able to remove with bus now
    private Optional<Thread> owningThread = Optional.empty();

    public ConsumerOffsetCommitter(final ConsumerManager<K, V> newConsumer, final WorkManager<K, V> newWorkManager, ParallelConsumerOptions options) {
        super(newConsumer, newWorkManager);
        commitMode = options.getCommitMode();
        commitTimeout = options.getOffsetCommitTimeout();
        if (commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER)) {
            throw new IllegalArgumentException("Cannot use " + commitMode + " when using " + this.getClass().getSimpleName());
        }
    }

    /**
     * Might block if using {@link CommitMode#PERIODIC_CONSUMER_SYNC}
     *
     * @see CommitMode
     */
    // todo this should be package private, and should not need to expose a thread safe interface - bubble up to broker system instead - see Controller refactor
    void commit(CommitData offsetsToCommit) throws TimeoutException, InterruptedException {
        if (isCurrentThreadOwner()) {
            // todo why? when am i the owner? audit - i think never
            // commit directly with consumer
            retrieveOffsetsAndCommit(offsetsToCommit);
        }

        Future<Class<Void>> ask = commitRequestSend(offsetsToCommit);

        if (isSync()) {
            log.debug("Sync commit - waiting on a commit response");
            try {
                ask.get(commitTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                throw new InternalRuntimeException(msg("Timeout waiting for commit response {} to request {}", commitTimeout));
            }
            log.debug("Finished waiting");
        } else {
            // async - fire and forget
            log.debug("Async commit sent");
        }
    }

    @Override
    protected void commitOffsets(CommitData offsetsToSend, ConsumerGroupMetadata groupMetadata) {
        if (offsetsToSend.isEmpty()) {
            log.trace("Nothing to commit");
            return;
        }
        switch (commitMode) {
            case PERIODIC_CONSUMER_SYNC -> {
                log.debug("Committing offsets Sync");
                consumerMgr.commitSync(offsetsToSend);
            }
            case PERIODIC_CONSUMER_ASYNCHRONOUS -> {
                //
                log.debug("Committing offsets Async");
                consumerMgr.commitAsync(offsetsToSend.getOffsetsToCommit(), (offsets, exception) -> {
                    if (exception != null) {
                        log.error("Error committing offsets", exception);
                        // todo keep work in limbo until async response is received?
                    }
                });
            }
            default ->
                    throw new IllegalArgumentException("Cannot use " + commitMode + " when using " + this.getClass().getSimpleName());
        }
    }

    /**
     * @see #commit()
     */
    @Override
    protected void postCommit() {
    }

    /**
     * @deprecated todo still needed? audit
     */
    @Deprecated
    private boolean isCurrentThreadOwner() {
        return Thread.currentThread().equals(owningThread.orElse(null));
    }

    private final ActorImpl<ConsumerOffsetCommitter<K, V>> myActor = new ActorImpl<>(this);

//    @SneakyThrows // remove
//    private void commitAndWait() {
//        Future<Class<Void>> ask = commitRequestSend();
//
//        log.debug("Waiting on a commit response");
//        ask.get(commitTimeout.toMillis(), TimeUnit.MILLISECONDS);

//        // \/ old version!
//
//        // request
//        CommitRequest commitRequest = requestCommitInternal();
//
//        // wait
//        boolean waitingOnCommitResponse = true;
//        int attempts = 0;
//        while (waitingOnCommitResponse) {
//            if (attempts > ARBITRARY_RETRY_LIMIT)
//                throw new InternalRuntimeError("Too many attempts taking commit responses");
//
//            try {
//                log.debug("Waiting on a commit response");
//                Duration timeout = AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT;
//                CommitResponse take = commitResponseQueue.poll(commitTimeout.toMillis(), TimeUnit.MILLISECONDS); // blocks, drain until we find our response
//                if (take == null)
//                    throw new InternalRuntimeError(msg("Timeout waiting for commit response {} to request {}", timeout, commitRequest));
//                waitingOnCommitResponse = take.getRequest().getId() != commitRequest.getId();
//            } catch (InterruptedException e) {
//                log.debug("Interrupted waiting for commit response", e);
//            }
//            attempts++;
//        }
//    }

    private Future<Class<Void>> commitRequestSend(CommitData offsetsToCommit) {
        // change to ask with ack, but needs throwing Consumer?
        return myActor.ask(committer -> {
            committer.retrieveOffsetsAndCommit(offsetsToCommit);
            return Void.class;
        });
    }

    // removed as the commiter will do the commit directly if instructed through messaging
//    private CommitRequest requestCommitInternal() {
//        CommitRequest request = new CommitRequest();
//        commitRequestQueue.add(request);
//        consumerMgr.wakeup();
//        return request;
//    }

    // removed as the commiter will do the commit directly if instructed through messaging
    void maybeDoCommit(CommitData offsetsToCommit) throws TimeoutException, InterruptedException {
//        // todo poll mail box instead
//        CommitRequest poll = commitRequestQueue.poll();
//        if (poll != null) {
//            log.debug("Commit requested, performing...");
//            retrieveOffsetsAndCommit(offsetsToCommit);
//            // only need to send a response if someone will be waiting
//            if (isSync()) {
//                log.debug("Adding commit response to queue...");
//                commitResponseQueue.add(new CommitResponse(poll));
//            }
//        }
    }

    public boolean isSync() {
        return commitMode.equals(PERIODIC_CONSUMER_SYNC);
    }

    public void claim() {
        owningThread = Optional.of(Thread.currentThread());
    }
}
