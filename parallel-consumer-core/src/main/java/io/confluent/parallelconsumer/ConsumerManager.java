package io.confluent.parallelconsumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import pl.tlinkowski.unij.api.UniMaps;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Delegate for {@link KafkaConsumer}
 * <p>
 * Also wrapped for thread saftey.
 */
@Slf4j
//@RequiredArgsConstructor
public class ConsumerManager<K, V> implements AutoCloseable {

    private final Consumer<K, V> consumer;

    //    private final Semaphore consumerLock = new Semaphore(1);
    private final ReentrantLock consumerLock = new ReentrantLock(true);

    private final AtomicBoolean pollingBroker = new AtomicBoolean(false);
//    private final ConsumerOffsetCommitter<K, V> consumerCommitter;

    private int erroneousWakups = 0;
    private int correctPollWakeups = 0;
    private int noWakeups = 0;
    private boolean commitRequested;

    public ConsumerManager(final Consumer<K, V> consumer
//            ,
//                           final ConsumerOffsetCommitter<K, V> consumerCommitter,
//                           final ParallelConsumerOptions options
    ) {
        this.consumer = consumer;
//        this.consumerCommitter = consumerCommitter;
    }

    ConsumerRecords<K, V> poll(Duration requestedLongPollTimeout) {
        return doWithConsumer(() -> {
            Duration timeoutToUse = requestedLongPollTimeout;
            ConsumerRecords<K, V> records;
            try {
                if (commitRequested) {
                    log.debug("Commit requested, so will not long poll as need to perform the commit");
                    timeoutToUse = Duration.ofMillis(1);// disable long poll, as commit needs performing
                    commitRequested = false;
                }
                pollingBroker.set(true);
                records = consumer.poll(timeoutToUse);
                log.debug("Poll completed normally and returned {}...", records.count());
            } catch (WakeupException w) {
                correctPollWakeups++;
                log.debug("Awoken from broker poll");
                log.trace("Wakeup caller is:", w);
                records = new ConsumerRecords<>(UniMaps.of());
            } finally {
                pollingBroker.set(false);
            }
            return records;
        });
    }

    /**
     * Wakes up the consumer, but only if it's polling.
     * <p>
     * Otherwise we can interrupt other operations like {@link KafkaConsumer#commitSync()}.
     */
    public void wakeup() {
        // boolean reduces the chances of a mis-timed call to wakeup, but doesn't prevent all spurious wake up calls to other methods like #commit
        // if the call to wakeup happens /after/ the check for a wake up state inside #poll, then the next call will through the wake up exception (i.e. #commit)
        if (pollingBroker.get()) {
            log.debug("Waking up consumer");
//            doWithConsumer(consumer::wakeup);
            consumer.wakeup();
        }
    }

//    Thread consumerWrapThread = new Thread();

    private void doWithConsumer(final Runnable o) {
        try {
            aquireLock();
        } catch (InterruptedException e) {
            throw new InternalRuntimeError("Error locking consumer", e);
        }
        try {
            o.run();
        } catch (Exception exception) {
            throw new InternalRuntimeError("Unknown", exception);
        } finally {
            releaseLock();
        }
    }

    private <R> R doWithConsumer(final Callable<R> o) {
        try {
            aquireLock();
        } catch (InterruptedException e) {
            throw new InternalRuntimeError("Error locking consumer", e);
        }
        try {
            return o.call();
        } catch (Exception exception) {
            throw new InternalRuntimeError("Unknown", exception);
        } finally {
            releaseLock();
        }
    }

    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        // we dont' want to be woken up during a commit, only polls
        boolean inProgress = true;
        noWakeups++;
        while (inProgress) {
            try {
                doWithConsumer(() -> consumer.commitSync(offsetsToSend));
                inProgress = false;
            } catch (WakeupException w) {
                log.debug("Got woken up, retry. errors: " + erroneousWakups + " none: " + noWakeups + " correct:" + correctPollWakeups, w);
                erroneousWakups++;
            }
        }
    }

    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        // we dont' want to be woken up during a commit, only polls
        boolean inProgress = true;
        noWakeups++;
        while (inProgress) {
            try {
//                List<Thread> collect = Thread.getAllStackTraces().keySet().stream().filter(x -> x.getId() == 46L).collect(Collectors.toList());
//                log.info("46:{}, {}", collect.get(0), consumerLock.isHeldByCurrentThread());
                doWithConsumer(() ->
                        consumer.commitAsync(offsets, callback)
                );
                inProgress = false;
            } catch (WakeupException w) {
                log.debug("Got woken up, retry. errors: " + erroneousWakups + " none: " + noWakeups + " correct:" + correctPollWakeups, w);
                erroneousWakups++;
            }
        }
    }

    public ConsumerGroupMetadata groupMetadata() {
        return doWithConsumer(consumer::groupMetadata);
    }

    public void close(final Duration defaultTimeout) {
        doWithConsumer(() -> consumer.close(defaultTimeout));
    }

    public Set<TopicPartition> assignment() {
        return doWithConsumer(consumer::assignment);
    }

    public void pause(final Set<TopicPartition> assignment) {
        doWithConsumer(() -> consumer.pause(assignment));
    }

    public Set<TopicPartition> paused() {
        return doWithConsumer(consumer::paused);
    }

    public void resume(final Set<TopicPartition> pausedTopics) {
        doWithConsumer(() -> consumer.resume(pausedTopics));
    }


    /**
     * Nasty reflection to check if auto commit is disabled.
     * <p>
     * Other way would be to politely request the user also include their consumer properties when construction, but
     * this is more reliable in a correctness sense, but britle in terms of coupling to internal implementation.
     * Consider requesting ability to inspect configuration at runtime.
     */
    @SneakyThrows
    public void checkAutoCommitIsDisabled() {
//        doWithConsumer(() -> {
        if (consumer instanceof KafkaConsumer) {
            // Commons lang FieldUtils#readField - avoid needing commons lang
            Field coordinatorField = consumer.getClass().getDeclaredField("coordinator"); //NoSuchFieldException
            coordinatorField.setAccessible(true);
            ConsumerCoordinator coordinator = (ConsumerCoordinator) coordinatorField.get(consumer); //IllegalAccessException

            Field autoCommitEnabledField = coordinator.getClass().getDeclaredField("autoCommitEnabled");
            autoCommitEnabledField.setAccessible(true);
            Boolean isAutoCommitEnabled = (Boolean) autoCommitEnabledField.get(coordinator);

            if (isAutoCommitEnabled)
                throw new IllegalStateException("Consumer auto commit must be disabled, as commits are handled by the library.");
        } else {
            // noop - probably MockConsumer being used in testing - which doesn't do auto commits
        }
//        });
    }

    public void onCommitRequested() {
        this.commitRequested = true;
    }
    public void checkNotSubscribed() {
        if (consumer instanceof MockConsumer)
            // disabled for unit tests which don't test rebalancing
            return;
        doWithConsumer(() -> {
            Set<String> subscription = consumer.subscription();
            Set<TopicPartition> assignment = consumer.assignment();

            if (!subscription.isEmpty() || !assignment.isEmpty()) {
                throw new IllegalStateException("Consumer subscription must be managed by this class. Use " + this.getClass().getName() + "#subcribe methods instead.");
            }
        });
    }

    //    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        log.debug("Subscribing to {}", topics);
        doWithConsumer(() -> consumer.subscribe(topics, callback));
    }

    //    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        log.debug("Subscribing to {}", pattern);
        doWithConsumer(() -> consumer.subscribe(pattern, callback));
    }

    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> assignment) {
        return consumer.committed(assignment);
    }

    void aquireLock() throws InterruptedException {
        log.debug("Locking");
//        if (consumerLock.tryLock()) {
        wakeup();
        consumerLock.lock();
//        } else {
//            throw new InternalRuntimeError("Deadlock");
//        }
    }

    void releaseLock() {
        log.debug("Unlocking");
        consumerLock.unlock();
    }

    @Override
    public void close() throws Exception {
//        releaseLock();
        consumer.close();
    }
}
