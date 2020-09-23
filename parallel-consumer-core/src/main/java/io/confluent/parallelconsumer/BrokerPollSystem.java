package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Subsystem for polling the broker for messages.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class BrokerPollSystem<K, V> {

    final private org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    public ParallelConsumer.State state = ParallelConsumer.State.running;

    private Optional<Future<Boolean>> pollControlThreadFuture;

    volatile private boolean paused = false;

    private final ParallelConsumer<K, V> async;

    @Setter
    @Getter
    static private Duration longPollTimeout = Duration.ofMillis(2000);

    final private WorkManager<K, V> wm;

    public BrokerPollSystem(Consumer<K, V> consumer, WorkManager<K, V> wm, ParallelConsumer<K, V> async) {
        this.consumer = consumer;
        this.wm = wm;
        this.async = async;
    }

    public void start() {
        this.pollControlThreadFuture = Optional.of(Executors.newSingleThreadExecutor().submit(this::controlLoop));
    }

    /**
     * @return true if closed cleanly
     */
    private boolean controlLoop() {
        Thread.currentThread().setName("broker-poll");
        log.trace("Broker poll control loop start");
        while (state != ParallelConsumer.State.closed) {
            log.trace("Loop: Poll broker");
            ConsumerRecords<K, V> polledRecords = pollBrokerForRecords();

            if (!polledRecords.isEmpty()) {
                log.trace("Loop: Register work");
                wm.registerWork(polledRecords);

                // notify control work has been registered
                async.notifyNewWorkRegistered();
            }

            switch (state) {
                case draining -> {
                    doPause();
                    // transition to closing
                    state = ParallelConsumer.State.closing;
                }
                case closing -> {
                    if (polledRecords.isEmpty()) {
                        doClose();
                    } else {
                        log.info("Subscriptions are paused, but records are still being drained (count: {})", polledRecords.count());
                    }
                }
            }
        }
        log.trace("Broker poll thread returning true");
        return true;
    }

    private void doClose() {
        log.debug("Closing {}, first closing consumer...", this.getClass().getSimpleName());
        this.consumer.close(ParallelConsumer.defaultTimeout);
        log.debug("Consumer closed.");
        state = ParallelConsumer.State.closed;
    }

    private ConsumerRecords<K, V> pollBrokerForRecords() {
        managePauseOfSubscription();

        Duration thisLongPollTimeout = (state == ParallelConsumer.State.running) ? BrokerPollSystem.longPollTimeout : Duration.ofMillis(1); // Can't use Duration.ZERO - this causes Object#wait to wait forever

        log.debug("Long polling broker with timeout {} seconds, might appear to sleep here if no data available on broker.", toSeconds(thisLongPollTimeout)); // java 8
        ConsumerRecords<K, V> records;
        try {
            records = this.consumer.poll(thisLongPollTimeout);
            log.debug("Poll completed normally and returned {}...", records.count());
        } catch (WakeupException w) {
            log.warn("Awoken from poll. State? {}", state);
            records = new ConsumerRecords<>(UniMaps.of());
        }
        return records;
    }

    /**
     * Will begin the shutdown process, eventually closing itself once drained
     */
    public void drain() {
        // idempotent
        if (state != ParallelConsumer.State.draining) {
            log.debug("Poll system signaling to drain...");
            state = ParallelConsumer.State.draining;
            consumer.wakeup();
        }
    }

    private void doPause() {
        // idempotent
        if (paused) {
            log.trace("Already paused");
        } else {
            paused = true;
            log.debug("Pausing subs");
            Set<TopicPartition> assignment = consumer.assignment();
            consumer.pause(assignment);
        }
    }

    public void closeAndWait() throws TimeoutException, ExecutionException {
        log.debug("Requesting broker polling system to close...");
        transitionToClosing();
        if (pollControlThreadFuture.isPresent()) {
            log.debug("Wait for loop to finish ending...");
            Future<Boolean> pollControlResult = pollControlThreadFuture.get();
            boolean interrupted = true;
            while(interrupted) {
                try {
                    Boolean pollShutdownSuccess = pollControlResult.get(ParallelConsumer.defaultTimeout.toMillis(), MILLISECONDS);
                    interrupted = false;
                    if (!pollShutdownSuccess) {
                        log.warn("Broker poll control thread not closed cleanly.");
                    }
                } catch (InterruptedException e) {
                    log.debug("Interrupted", e);
                } catch (ExecutionException | TimeoutException e) {
                    log.error("Execution or timeout exception", e);
                    throw e;
                }
            }
        }
        log.debug("Broker poll system finished closing");
    }

    private void transitionToClosing() {
        state = ParallelConsumer.State.closing;
        consumer.wakeup();
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
            log.debug("Resuming consumer");
            Set<TopicPartition> pausedTopics = consumer.paused();
            consumer.resume(pausedTopics);
            // trigger consumer to perform a new poll without the assignments paused, otherwise it will continue to long poll on nothing
            consumer.wakeup();
            paused = false;
        }
    }

    private boolean shouldThrottle() {
        return wm.shouldThrottle();
    }
}
