package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * @author Antony Stubbs
 * @see FullConsumerFacade
 */
@Slf4j
@ThreadSafe
public class FullConsumerFacadeImpl<K, V> extends ConsumerFacadeForPCImpl<K, V> implements FullConsumerFacade<K, V> {

    /**
     * How to handle unsupported opertaions - error or No-op/swallow.
     */
    UnsupportedReaction reaction = UnsupportedReaction.ERROR;

    public FullConsumerFacadeImpl(ParallelConsumer<K, V> pcApi, BrokerPollSystem<K, V> poller) {
        super(pcApi, poller);
    }

    @Override
    public void setReactionMode(UnsupportedReaction unsupportedReactionMode) {
        this.reaction = unsupportedReactionMode;
    }

    // todo branch feature - optionally have the implememtation talk directly to the work manager and "poll" records from it - PC can look exactly like a normal consumer?
    @Override
    public ConsumerRecords<K, V> poll(final long timeout) {
        throwInvalidCall();
        return ConsumerRecords.empty();
    }

    private void throwInvalidCall() throws ParallelConsumerException {
        if (reaction.equals(UnsupportedReaction.SWALLOW)) {
            log.trace("Swallowing invalid call to a method");
        } else {
            throw new ParallelConsumerException("Function not allowed, not implemented or can't implement");
        }
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

    public enum UnsupportedReaction {
        /**
         * Throw an exception if an unsupported operation is called.
         */
        ERROR,
        /**
         * No-op if an unsupported operation is called.
         */
        SWALLOW
    }
}
