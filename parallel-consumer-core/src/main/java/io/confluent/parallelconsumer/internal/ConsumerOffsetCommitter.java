package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

    /**
     * Chosen arbitrarily - retries should never be needed, if they are it's an invalid state
     */
    private static final int ARBITRARY_RETRY_LIMIT = 50;

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

    public ConsumerOffsetCommitter(final ConsumerManager<K, V> newConsumer, final WorkManager<K, V> newWorkManager, final ParallelConsumerOptions options) {
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
    void commit() {
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
    protected void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata) {
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
    }

    private boolean isOwner() {
        return Thread.currentThread().equals(owningThread.orElse(null));
    }

    /**
     * Commit request message
     */
    @Value
    static class CommitRequest {
        UUID id = UUID.randomUUID();
        long requestedAtMs = System.currentTimeMillis();
    }

    /**
     * Commit response message, linked to a {@link CommitRequest}
     */
    @Value
    static class CommitResponse {
        CommitRequest request;
    }

    private void commitAndWait() {
        // request
        CommitRequest commitRequest = requestCommitInternal();

        // wait
        boolean waitingOnCommitResponse = true;
        int attempts = 0;
        while (waitingOnCommitResponse) {
            if (attempts > ARBITRARY_RETRY_LIMIT)
                throw new InternalRuntimeError("Too many attempts taking commit responses");

            try {
                log.debug("Waiting on a commit response");
                Duration timeout = AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT;
                CommitResponse take = commitResponseQueue.poll(commitTimeout.toMillis(), TimeUnit.MILLISECONDS); // blocks, drain until we find our response
                if (take == null)
                    throw new InternalRuntimeError(msg("Timeout waiting for commit response {} to request {}", timeout, commitRequest));
                waitingOnCommitResponse = take.getRequest().getId() != commitRequest.getId();
            } catch (InterruptedException e) {
                log.debug("Interrupted waiting for commit response", e);
            }
            attempts++;
        }
    }

    private CommitRequest requestCommitInternal() {
        CommitRequest request = new CommitRequest();
        commitRequestQueue.add(request);
        consumerMgr.wakeup();
        return request;
    }

    void maybeDoCommit() {
        CommitRequest poll = commitRequestQueue.poll();
        if (poll != null) {
            log.debug("Commit requested, performing...");
            retrieveOffsetsAndCommit();
            // only need to send a response if someone will be waiting
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
