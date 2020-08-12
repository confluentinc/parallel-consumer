package io.confluent.csid.asyncconsumer;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.confluent.csid.asyncconsumer.AsyncConsumer.State.*;
import static io.confluent.csid.asyncconsumer.AsyncConsumer.defaultTimeout;
import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Subsystem for polling the broker for messages.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class BrokerPollSystem<K, V> {

    final private org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    public AsyncConsumer.State state = AsyncConsumer.State.running;

    private Optional<Future<Boolean>> controlFuture;

    volatile private boolean paused = false;

    private AsyncConsumer async;

    @Setter
    @Getter
    static private Duration longPollTimeout = Duration.ofMillis(2000);

    final private WorkManager<K, V> wm;

    public BrokerPollSystem(Consumer<K, V> consumer, WorkManager<K, V> wm, AsyncConsumer async) {
        this.consumer = consumer;
        this.wm = wm;
        this.async = async;
    }

    public void start() {
        this.controlFuture = Optional.of(Executors.newSingleThreadExecutor().submit(this::controlLoop));
    }

    private boolean controlLoop() {
        Thread.currentThread().setName("broker-poll");
        log.trace("Broker poll control loop start");
        while (state != closed) {
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
                    state = closing;
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
        return true;
    }

    private void doClose() {
        log.debug("Close consumer...");
        this.consumer.close(defaultTimeout);
        log.debug("Closed.");
        state = closed;
    }

    private ConsumerRecords<K, V> pollBrokerForRecords() {
        managePauseOfSubscription();

        Duration thisLongPollTimeout = (state == running) ? BrokerPollSystem.longPollTimeout : Duration.ofMillis(1); // Can't use Duration.ZERO - this causes Object#wait to wait forever

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
        if (state != AsyncConsumer.State.draining) {
            log.debug("Signaling to drain...");
            state = draining;
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

    @SneakyThrows
    public void closeAndWait() {
        log.debug("Requesting broker polling system to close...");
        transitionToClosing();
        if (controlFuture.isPresent()) {
            log.debug("Wait for loop to finish ending...");
            Boolean success = controlFuture.get()
                    .get(defaultTimeout.toMillis(), MILLISECONDS);
        }
        log.debug("Broker poll system finished closing");
    }

    private void transitionToClosing() {
        state = AsyncConsumer.State.closing;
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
