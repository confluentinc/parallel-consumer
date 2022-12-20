package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * Implements all of the {@link Consumer} interface, either asynchronously, or throws an exception if the function is
 * not supported.
 * <p>todo resolve docs</p>
 * An implementation of the Kafka Consumer API, with unsupported functions which throw exceptions. Useful for when you
 * want to let something else use PC, where it's unaware of it's capabilities, yet you know it won't call unsupported
 * methods.
 * <p>
 * Generally, you can only... And you can't...
 *
 * @author Antony Stubbs
 */
@Slf4j
public class FullConsumerFacade<K, V> extends ConsumerFacade<K, V> implements Consumer<K, V> {

    /**
     * todo docs
     */
    boolean swallow = false;

    public FullConsumerFacade(BrokerPollSystem<K, V> basePollerRef) {
        super(basePollerRef);
    }

    // todo branch feature - optionally have the implememtation talk directly to the work manager and "poll" records from it - PC can look exactly like a normal consumer?
    @Override
    public ConsumerRecords<K, V> poll(final long timeout) {
        throwInvalidCall();
        return ConsumerRecords.empty();
    }

    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        throwInvalidCall();
        return ConsumerRecords.empty();
    }

    // todo any universe where allowing direct offset committing makes sense? might act like a sort of seek?
    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        throwInvalidCall();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        throwInvalidCall();
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        throwInvalidCall();
    }

    // todo pass through to manually "blocking" or even really pausing partitions?
    @Override
    public void pause(final Collection collection) {
        throwInvalidCall();
    }

    @Override
    public void resume(final Collection collection) {
        throwInvalidCall();
    }

    // audit
    @Override
    public void enforceRebalance() {
        throwInvalidCall(); // ?
//        return blockingAskConsumer(consumer -> consumer.enforceRebalance(partition, timeout));
    }

    // audit
    @Override
    public void enforceRebalance(String reason) {
        throwInvalidCall(); // ?
    }

    // option to have this shutdown PC?
    @Override
    public void close() {
        throwInvalidCall();
    }

    @Override
    public void close(final Duration timeout) {
        throwInvalidCall();
    }

    /**
     * todo docs
     */
    private void throwInvalidCall() throws ParallelConsumerException {
        if (swallow) {
            log.trace("Swallowing invalid call to a method");
        } else {
            throw new ParallelConsumerException("Not allowed");
        }
    }
}
