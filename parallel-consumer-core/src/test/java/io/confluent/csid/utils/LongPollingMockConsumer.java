package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniMaps;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Used in tests to stub out the behaviour of the real Broker and Client's long polling system (the mock Kafka Consumer
 * doesn't have this behaviour).
 *
 * @author Antony Stubbs
 */
@ToString
@Slf4j
public class LongPollingMockConsumer<K, V> extends MockConsumer<K, V> {

    // thread safe for easy parallel tests - no need for performance considerations as is test harness
    @Getter
    private final CopyOnWriteArrayList<Map<TopicPartition, OffsetAndMetadata>> commitHistoryInt = new CopyOnWriteArrayList<>();

    public LongPollingMockConsumer(OffsetResetStrategy offsetResetStrategy) {
        super(offsetResetStrategy);
    }

    private final AtomicBoolean statePretendingToLongPoll = new AtomicBoolean(false);

    @Override
    public synchronized ConsumerRecords<K, V> poll(Duration timeout) {
        var records = super.poll(timeout);

        if (records.isEmpty()) {
            log.debug("No records returned, simulating long poll with sleep for requested long poll timeout of {}...", timeout);
            synchronized (this) {
                var sleepUntil = Instant.now().plus(timeout);
                statePretendingToLongPoll.set(true);
                while (statePretendingToLongPoll.get() && !timeoutReached(sleepUntil)) {
                    Duration left = Duration.between(Instant.now(), sleepUntil);
                    log.debug("Time remaining: {}", left);
                    try {
                        // a sleep of 0 ms causes an indefinite sleep
                        long msLeft = left.toMillis();
                        if (msLeft > 0) {
                            this.wait(msLeft);
                        }
                    } catch (InterruptedException e) {
                        log.warn("Interrupted, ending this long poll early", e);
                        statePretendingToLongPoll.set(false);
                    }
                }
                if (statePretendingToLongPoll.get() && !timeoutReached(sleepUntil)) {
                    log.debug("Don't know why I was notified to wake up");
                } else if (statePretendingToLongPoll.get() && timeoutReached(sleepUntil)) {
                    log.debug("Simulated long poll of ({}) finished. Now: {} vs sleep until: {}", timeout, Instant.now(), sleepUntil);
                } else if (!statePretendingToLongPoll.get()) {
                    log.debug("Simulated long poll was interrupted by by WAKEUP command...");
                }
                statePretendingToLongPoll.set(false);
            }
        } else {
            log.debug("Polled and found {} records...", records.count());
        }
        return records;
    }

    /**
     * Restricted to ms precision to match {@link #wait()} semantics
     */
    private boolean timeoutReached(Instant sleepUntil) {
        long now = Instant.now().toEpochMilli();
        long until = sleepUntil.toEpochMilli();
        // plus one due to truncation/rounding semantics
        return now + 1 >= until;
    }

    @Override
    public synchronized void addRecord(ConsumerRecord<K, V> record) {
        super.addRecord(record);
        // wake me up if I'm pretending to long poll
        wakeup();
    }

    @Override
    public synchronized void wakeup() {
        if (statePretendingToLongPoll.get()) {
            statePretendingToLongPoll.set(false);
            log.debug("Interrupting mock long poll...");
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

    @Override
    public synchronized void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        commitHistoryInt.add(offsets);
        super.commitAsync(offsets, callback);
    }

    /**
     * Makes the commit history look like the {@link MockProducer}s one so we can use the same assert method.
     *
     * @see KafkaTestUtils#assertCommitLists(List, List, Optional)
     */
    private List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> injectConsumerGroupId(final List<Map<TopicPartition, OffsetAndMetadata>> commitHistory) {
        String groupId = this.groupMetadata().groupId();
        return commitHistory.stream()
                .map(x -> UniMaps.of(groupId, x))
                .collect(Collectors.toList());
    }

    /*
     * Makes the commit history look like the {@link MockProducer}s one, so we can use the same assert method.
     *
     * @see KafkaTestUtils#assertCommitLists(List, List, Optional)
     */
    public List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> getCommitHistoryWithGroupId() {
        var commitHistoryInt = getCommitHistoryInt();
        return injectConsumerGroupId(commitHistoryInt);
    }

    @Override
    @SneakyThrows
    public synchronized void close(Duration timeout) {
        revokeAssignment();
        super.close(timeout);
    }

    /**
     * {@link MockConsumer} fails to implement any {@link ConsumerRebalanceListener} system, so we manually revoke
     * assignments, use reflection to access the registered rebalance listener, call the listener, and only then close
     * the consumer.
     *
     * @see AbstractParallelEoSStreamProcessor#onPartitionsRevoked
     */
    private void revokeAssignment() throws NoSuchFieldException, IllegalAccessException {
        ConsumerRebalanceListener consumerRebalanceListener = getRebalanceListener();

        // execute
        if (consumerRebalanceListener == null) {
            log.warn("No rebalance listener assigned - on revoke can't fire");
        } else {
            Set<TopicPartition> assignment = super.assignment();
            consumerRebalanceListener.onPartitionsRevoked(assignment);
        }
    }

    private ConsumerRebalanceListener getRebalanceListener() throws NoSuchFieldException, IllegalAccessException {
        // access listener
        Field subscriptionsField = MockConsumer.class.getDeclaredField("subscriptions"); //NoSuchFieldException
        subscriptionsField.setAccessible(true);
        SubscriptionState subscriptionState = (SubscriptionState) subscriptionsField.get(this); //IllegalAccessException
        ConsumerRebalanceListener consumerRebalanceListener = subscriptionState.rebalanceListener();
        return consumerRebalanceListener;
    }

    public void subscribeWithRebalanceAndAssignment(final List<String> topics, int partitions) {
        List<TopicPartition> topicPartitions = topics.stream()
                .flatMap(y -> IntStream.range(0, partitions).boxed()
                        .map(x -> new TopicPartition(y, x)))
                .collect(Collectors.toList());
        rebalance(topicPartitions);

        //
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        for (var tp : topicPartitions) {
            beginningOffsets.put(tp, 0L);
        }
        super.updateBeginningOffsets(beginningOffsets);
    }

    @SneakyThrows
    public void revoke(final Collection<TopicPartition> newAssignment) {
        ConsumerRebalanceListener rebalanceListeners = getRebalanceListener();
        if (rebalanceListeners != null) {
            rebalanceListeners.onPartitionsRevoked(newAssignment);
        }
    }

    @Override
    @SneakyThrows
    public synchronized void assign(final Collection<TopicPartition> newAssignment) {
        ConsumerRebalanceListener rebalanceListeners = getRebalanceListener();
        if (rebalanceListeners != null) {
            rebalanceListeners.onPartitionsAssigned(newAssignment);
        }
    }

    public synchronized void rebalanceWithoutAssignment(final Collection<TopicPartition> newAssignment) {
        super.rebalance(newAssignment);
    }
}
