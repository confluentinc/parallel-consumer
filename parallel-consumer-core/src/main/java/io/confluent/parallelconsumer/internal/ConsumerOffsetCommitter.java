package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerException;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.*;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.internal.ConsumerManager.DEFAULT_API_TIMEOUT;

/**
 * Committer that uses the Kafka Consumer to commit either synchronously or asynchronously
 *
 * @see CommitMode
 */
@Slf4j
public class ConsumerOffsetCommitter<K, V> extends AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    /**
     * Chosen arbitrarily - retries should never be needed, if they are it's an invalid state
     */
    private static final int ARBITRARY_RETRY_LIMIT = 50;

    private final ParallelConsumerOptions<K, V> options;

    private final CommitMode commitMode;

    private final Duration commitTimeout;

    private Optional<Thread> owningThread = Optional.empty();

    /**
     * Queue of commit requests from other threads
     */
    private final Queue<CommitRequest> commitRequestQueue = new ConcurrentLinkedQueue<>();

    /**
     * Queue of commit responses, for other threads to block on
     */
    private final BlockingQueue<CommitResponse> commitResponseQueue = new LinkedBlockingQueue<>();

    public ConsumerOffsetCommitter(final ConsumerManager<K, V> newConsumer, final WorkManager<K, V> newWorkManager, final ParallelConsumerOptions<K, V> options) {
        super(newConsumer, newWorkManager);
        commitMode = options.getCommitMode();
        commitTimeout = options.getOffsetCommitTimeout();
        this.options = options;
        if (commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER)) {
            throw new IllegalArgumentException("Cannot use " + commitMode + " when using " + this.getClass().getSimpleName());
        }
    }

    /**
     * Might block briefly if using {@link CommitMode#PERIODIC_CONSUMER_SYNC}
     *
     * @see CommitMode
     */
    void commit() throws
            TimeoutException, // java.util.concurrent.TimeoutException
            InterruptedException,
            PCTimeoutException,
            PCCommitFailedException {
        if (isOwner()) {
            retrieveOffsetsAndCommit();
        } else if (isSync()) {
            log.debug("Sync commit");
            commitAndWait();
            log.debug("Finished waiting");
        } else {
            // async
            // we just request the commit and hope
            log.debug("Async commit to be requested");
            requestCommitInternal();
        }
    }

    @Override
    protected void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata) throws PCTimeoutException, PCCommitFailedException {
        if (offsetsToSend.isEmpty()) {
            log.trace("Nothing to commit");
            return;
        }
        switch (commitMode) {
            case PERIODIC_CONSUMER_SYNC -> {
                consumerMgr.commitSync(offsetsToSend);
            }
            case PERIODIC_CONSUMER_ASYNCHRONOUS -> {
                //
                log.debug("Committing offsets Async");
                consumerMgr.commitAsync(offsetsToSend);
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
        // nothing needed
    }

    private boolean isOwner() {
        return Thread.currentThread().equals(owningThread.orElse(null));
    }

    /**
     * Commit request message
     */
    @Value
    public static class CommitRequest {
        UUID id = UUID.randomUUID();
        long requestedAtMs = System.currentTimeMillis();
    }

    /**
     * Commit response message, linked to a {@link CommitRequest}
     */
    @Value
    public static class CommitResponse {
        CommitRequest request;

        Optional<InternalException> exception;

        public CommitResponse(CommitRequest poll, InternalException e) {
            this.request = poll;
            this.exception = Optional.ofNullable(e);
        }

        public CommitResponse(CommitRequest poll) {
            this(poll, null);
        }

        public UUID getRequestId() {
            return getRequest().getId();
        }

        public boolean isResponseFor(CommitRequest commitRequest) {
            return getRequestId() == commitRequest.getId();
        }

        public boolean hasException() {
            return getException().isPresent();
        }
    }

    // todo replace with actor framework once merged improvements/lambda-actor-bus pr#325
    private void commitAndWait() throws PCCommitFailedException {
        boolean waitingOnCommitResponse = true;
        int failedCommitAttempts = 0;
        int attempts = 0;

        // request
        CommitRequest commitRequest = requestCommitInternal();

        while (waitingOnCommitResponse) {
            if (attempts > ARBITRARY_RETRY_LIMIT) {
                throw new InternalRuntimeException(msg("Too many attempts taking commit response from commit thread ({} attempts to commit)",
                        attempts));
            }

            // wait
            try {
                // add some extra time to the timeout, to allow for the commit to be requested and responded to
                var buffer = Duration.ofSeconds(10);
                final Duration timeoutWithBuffer = commitTimeout.plus(buffer);
                log.debug("Waiting on a commit response with timeout {} (base: {}, buffer: {})", timeoutWithBuffer, commitTimeout, buffer);
                CommitResponse commitResponse = commitResponseQueue.poll(timeoutWithBuffer.toMillis(), TimeUnit.MILLISECONDS); // blocks, drain until we find our response

                // guard
                if (commitResponse == null) {
                    throw new InternalRuntimeException(msg("Timeout ({}) waiting for commit response from internal committer to request {}",
                            commitTimeout, commitRequest));
                }

                boolean responseMatchesRequestWaitingOn = commitResponse.isResponseFor(commitRequest);

                // continue waiting if our response doesn't match
                waitingOnCommitResponse = !responseMatchesRequestWaitingOn;

                if (responseMatchesRequestWaitingOn && commitResponse.hasException()) {
                    failedCommitAttempts++;
                    var error = commitResponse.getException().get();

                    if (error instanceof PCCommitFailedException pcCommitFailedException) {
                        throw pcCommitFailedException;
                    } else {
                        var retrySettings = options.getRetrySettings();
                        if (retrySettings.isFailFastOrRetryExhausted(failedCommitAttempts)) {
                            throw new ParallelConsumerException(
                                    msg("Timeout committing offsets and retry exhausted - failed commit attempts: {} settings: {}",
                                            failedCommitAttempts,
                                            retrySettings),
                                    error);
                        } else {
                            Duration commitTimeout = DEFAULT_API_TIMEOUT;
                            log.warn("Timeout ({}) committing offsets, will retry (failed: {} times, settings: {})",
                                    commitTimeout,
                                    failedCommitAttempts,
                                    retrySettings);
                            // request again
                            commitRequest = requestCommitInternal();
                            waitingOnCommitResponse = true;
                        }
                    }
                }
            } catch (InterruptedException e) {
                log.debug("Interrupted waiting for commit response", e);
            }
            attempts++;
        }
    }

    private CommitRequest requestCommitInternal() {
        log.debug("Sending commit request to thread...");
        CommitRequest request = new CommitRequest();
        commitRequestQueue.add(request);
        consumerMgr.wakeup();
        return request;
    }

    void maybeDoCommit() throws
            TimeoutException, // java.util.concurrent.TimeoutException
            InterruptedException {
        CommitRequest poll = commitRequestQueue.poll();
        if (poll != null) {
            log.debug("Commit requested, performing...");
            try {
                retrieveOffsetsAndCommit();
            } catch (PCCommitFailedException e) {
                // only need to send a response if someone will be waiting
                if (isSync()) {
                    log.warn("Commit failed exception - cannot retry. Adding commit response to response queue...", e);
                    commitResponseQueue.add(new CommitResponse(poll, e));
                }
            } catch (PCTimeoutException e) {
                // only need to send a response if controller thread is waiting
                if (isSync()) {
                    log.warn("Adding commit response with the timeout exception to response queue...");
                    commitResponseQueue.add(new CommitResponse(poll, e));
                }
            }
            // only need to send a response if controller thread is waiting
            if (isSync()) {
                log.debug("Adding commit response to queue...");
                commitResponseQueue.add(new CommitResponse(poll));
            }
        }
    }

    public boolean isSync() {
        return commitMode.equals(PERIODIC_CONSUMER_SYNC);
    }

    public void claim() {
        owningThread = Optional.of(Thread.currentThread());
    }
}
