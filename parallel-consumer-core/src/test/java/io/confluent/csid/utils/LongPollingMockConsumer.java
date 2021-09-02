package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniMaps;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Used in tests to stub out the behaviour of the real Broker and Client's long polling system (the mock Kafka Consumer
 * doesn't have this behaviour).
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class LongPollingMockConsumer<K, V> extends MockConsumer<K, V> {

    // thread safe for easy parallel tests - no need for performance considerations as is test harness
    @Getter
    private final CopyOnWriteArrayList<Map<TopicPartition, OffsetAndMetadata>> commitHistoryInt = new CopyOnWriteArrayList<>();

    public LongPollingMockConsumer(OffsetResetStrategy offsetResetStrategy) {
        super(offsetResetStrategy);
    }

    @Override
    public synchronized ConsumerRecords<K, V> poll(Duration timeout) {
        var records = super.poll(timeout);

        if (records.isEmpty()) {
            log.debug("No records returned, simulating long poll with sleep for requested long poll timeout of {}...", timeout);
            try {
                synchronized (this) {
                    this.wait(timeout.toMillis());
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted", e);
            }
            log.debug("Simulated long poll of ({}) finished.", timeout);
        } else {
            log.debug("Polled and found {} records...", records.count());
        }
        return records;
    }

    @Override
    public synchronized void wakeup() {
        log.debug("Interrupting mock long poll...");
        synchronized (this) {
            this.notifyAll();
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
     * Makes the commit history look like the {@link MockProducer}s one so we can use the same assert method.
     *
     * @see KafkaTestUtils#assertCommitLists(List, List, Optional)
     */
    public List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> getCommitHistoryWithGropuId() {
        var commitHistoryInt = getCommitHistoryInt();
        return injectConsumerGroupId(commitHistoryInt);
    }

    @Override
    @SneakyThrows
    public synchronized void close(final long timeout, final TimeUnit unit) {
        revokeAssignment();
        super.close(timeout, unit);
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
                .flatMap(y -> Range.rangeStream(partitions).boxed()
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
    @Override
    public synchronized void rebalance(final Collection<TopicPartition> newAssignment) {
        super.rebalance(newAssignment);
        ConsumerRebalanceListener rebalanceListeners = getRebalanceListener();
        if (rebalanceListeners != null) {
            rebalanceListeners.onPartitionsAssigned(newAssignment);
        }
    }
}