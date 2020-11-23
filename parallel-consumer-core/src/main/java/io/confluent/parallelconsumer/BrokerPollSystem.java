package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.parallelconsumer.ParallelEoSStreamProcessor.State.closed;
import static io.confluent.parallelconsumer.ParallelEoSStreamProcessor.State.running;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Subsystem for polling the broker for messages.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class BrokerPollSystem<K, V> implements OffsetCommitter {

    private final ConsumerManager<K, V> consumerManager;

    private ParallelEoSStreamProcessor.State state = ParallelEoSStreamProcessor.State.running;

    private Optional<Future<Boolean>> pollControlThreadFuture;

    private volatile boolean paused = false;

    private final ParallelEoSStreamProcessor<K, V> pc;

    private Optional<ConsumerOffsetCommitter<K, V>> committer = Optional.empty();

    /**
     * Note how this relates to {@link BrokerPollSystem#getLongPollTimeout()} - if longPollTimeout is high and loading
     * factor is low, there may not be enough messages queued up to satisfy demand.
     */
    @Setter
    @Getter
    private static Duration longPollTimeout = Duration.ofMillis(2000);

    private final WorkManager<K, V> wm;

    public BrokerPollSystem(ConsumerManager<K, V> consumerMgr, WorkManager<K, V> wm, ParallelEoSStreamProcessor<K, V> pc, final ParallelConsumerOptions options) {
        this.wm = wm;
        this.pc = pc;

        this.consumerManager = consumerMgr;
        switch (options.getCommitMode()) {
            case CONSUMER_SYNC, CONSUMER_ASYNCHRONOUS -> {
                ConsumerOffsetCommitter<K, V> consumerCommitter = new ConsumerOffsetCommitter<>(consumerMgr, wm, options);
                committer = Optional.of(consumerCommitter);
            }
        }
    }

    public void start() {
        Future<Boolean> submit = Executors.newSingleThreadExecutor().submit(this::controlLoop);
        this.pollControlThreadFuture = Optional.of(submit);
    }

    public void supervise() {
        if (pollControlThreadFuture.isPresent()) {
            Future<Boolean> booleanFuture = pollControlThreadFuture.get();
            if (booleanFuture.isCancelled() || booleanFuture.isDone()) {
                try {
                    booleanFuture.get();
                } catch (Exception e) {
                    throw new InternalError("Error in " + BrokerPollSystem.class.getSimpleName() + " system.", e);
                }
            }
        }
    }

    /**
     * @return true if closed cleanly
     */
    private boolean controlLoop() {
        Thread.currentThread().setName("broker-poll");
        log.trace("Broker poll control loop start");
        committer.ifPresent(x -> x.claim());
        try {
            while (state != closed) {
                log.trace("Loop: Broker poller: ({})", state);
                if (state == running) {
                    ConsumerRecords<K, V> polledRecords = pollBrokerForRecords();

                    if (!polledRecords.isEmpty()) {
                        log.trace("Loop: Register work");
                        wm.registerWork(polledRecords);

                        // notify control work has been registered
                        pc.notifyNewWorkRegistered();
                    }
                }

                maybeDoCommit();

                switch (state) {
                    case draining -> {
                        doPause();
                        transitionToCloseMaybe();
                    }
                    case closing -> {
                        doClose();
                    }
                }
            }
            log.debug("Broker poll thread finished, returning true to future");
            return true;
        } catch (Exception e) {
            log.error("Unknown error", e);
            throw e;
        }
    }

    private void transitionToCloseMaybe() {
        // make sure everything is committed
        if (isResponsibleForCommits() && !wm.isRecordsAwaitingToBeCommitted()) {
            // transition to closing
            state = ParallelEoSStreamProcessor.State.closing;
        }
    }

    private void doClose() {
        doPause();
        maybeCloseConsumer();
        state = closed;
    }

    /**
     * To keep things simple, make sure the correct thread which can make a commit, is the one to close the consumer.
     * This way, if partitions are revoked, the commit can be made inline.
     */
    private void maybeCloseConsumer() {
        if (isResponsibleForCommits()) {
            log.debug("Closing {}, first closing consumer...", this.getClass().getSimpleName());
            this.consumerManager.close(DrainingCloseable.DEFAULT_TIMEOUT);
            log.debug("Consumer closed.");
        }
    }

    private boolean isResponsibleForCommits() {
        return committer.isPresent();
    }

    private ConsumerRecords<K, V> pollBrokerForRecords() {
        managePauseOfSubscription();

        Duration thisLongPollTimeout = (state == ParallelEoSStreamProcessor.State.running) ? BrokerPollSystem.longPollTimeout : Duration.ofMillis(1); // Can't use Duration.ZERO - this causes Object#wait to wait forever

        log.debug("Long polling broker with timeout {} seconds, might appear to sleep here if subs are paused, or no data available on broker.", toSeconds(thisLongPollTimeout)); // java 8
        return consumerManager.poll(thisLongPollTimeout);
    }

    /**
     * Will begin the shutdown process, eventually closing itself once drained
     */
    public void drain() {
        // idempotent
        if (state != ParallelEoSStreamProcessor.State.draining) {
            log.debug("Signaling poll system to drain, waking up consumer...");
            state = ParallelEoSStreamProcessor.State.draining;
            consumerManager.wakeup();
        }
    }

    private void doPause() {
        // idempotent
        if (paused) {
            log.trace("Already paused");
        } else {
            paused = true;
            log.debug("Pausing subs");
            Set<TopicPartition> assignment = consumerManager.assignment();
            consumerManager.pause(assignment);
        }
    }

    public void closeAndWait() throws TimeoutException, ExecutionException {
        log.debug("Requesting broker polling system to close...");
        transitionToClosing();
        if (pollControlThreadFuture.isPresent()) {
            log.debug("Wait for loop to finish ending...");
            Future<Boolean> pollControlResult = pollControlThreadFuture.get();
            boolean interrupted = true;
            while (interrupted) {
                try {
                    Boolean pollShutdownSuccess = pollControlResult.get(DrainingCloseable.DEFAULT_TIMEOUT.toMillis(), MILLISECONDS);
                    interrupted = false;
                    if (!pollShutdownSuccess) {
                        log.warn("Broker poll control thread not closed cleanly.");
                    }
                } catch (InterruptedException e) {
                    log.debug("Interrupted waiting for broker poller thread to finish", e);
                } catch (ExecutionException | TimeoutException e) {
                    log.error("Execution or timeout exception waiting for broker poller thread to finish", e);
                    throw e;
                }
            }
        }
        log.debug("Broker poll system finished closing");
    }

    private void transitionToClosing() {
        log.debug("Poller transitioning to closing, waking up consumer");
        state = ParallelEoSStreamProcessor.State.closing;
        consumerManager.wakeup();
    }

    /**
     * If we are currently processing too many records, we must stop polling for more from the broker. But we must also
     * make sure we maintain the keep alive with the broker so as not to cause a rebalance.
     */
    private void managePauseOfSubscription() {
        if (shouldThrottle()) {
            doPause();
        } else {
            resumeIfPaused();
        }
    }

    /**
     * Has no flap limit, always resume if we need to
     */
    private void resumeIfPaused() {
        // idempotent
        if (paused) {
            log.debug("Resuming consumer, waking up");
            Set<TopicPartition> pausedTopics = consumerManager.paused();
            consumerManager.resume(pausedTopics);
            // trigger consumer to perform a new poll without the assignments paused, otherwise it will continue to long poll on nothing
            consumerManager.wakeup();
            paused = false;
        }
    }

    private boolean shouldThrottle() {
        return wm.shouldThrottle();
    }

    /**
     * Optionally blocks. Threadsafe
     *
     * @see CommitMode
     */
    @SneakyThrows
    @Override
    public void retrieveOffsetsAndCommit() {
        // {@link Optional#ifPresentOrElse} only @since 9
        ConsumerOffsetCommitter<K, V> comitter = committer.orElseThrow(() -> {
            // shouldn't be here
            throw new IllegalStateException("No committer configured");
        });
        comitter.commit();
    }

    /**
     * Will silently skip if not configured with a committer
     */
    private void maybeDoCommit() {
        committer.ifPresent(ConsumerOffsetCommitter::maybeDoCommit);
    }

    /**
     * Wakeup if colling the broker
     */
    public void wakeup() {
        consumerManager.wakeup();
    }
}
