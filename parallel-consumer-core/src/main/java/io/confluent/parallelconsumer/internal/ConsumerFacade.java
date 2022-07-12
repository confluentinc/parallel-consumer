package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerException;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Pattern;

import static io.confluent.parallelconsumer.internal.DrainingCloseable.DEFAULT_TIMEOUT;

/**
 * todo docs
 * <p>
 * This exposes a limited subset of the {@link Consumer} interface that is valid to be used within the PC context.
 * <p>
 * Generally, you can only... And you can't...
 * <p>
 * All methods are / must be thread safe.
 *
 * @author Antony Stubbs
 */
// todo rename suffix Async (is it must implement consumer, but is an ASYNC implementation)
@SuppressWarnings(
        {"ClassWithTooManyMethods",// ain't my interface
                "ClassCanBeRecord"}
)
@Slf4j
@RequiredArgsConstructor
// todo which package? root or internal?
public class ConsumerFacade<K, V> implements Consumer<K, V>, PCConsumerAPI<K, V> {

    //    private final AbstractParallelEoSStreamProcessor<?, ?> controller;
    private final BrokerPollSystem<K, V> basePollerRef;

    /**
     * Makes a blocking call to the consumer thread - will return once the other thread has looped over it's control
     * loop. Uses a default timeout of {@link io.confluent.parallelconsumer.internal.DrainingCloseable.DEFAULT_TIMEOUT}
     */
    @SneakyThrows
    @Override
    public Set<TopicPartition> assignment() {
        return blockingAskConsumer(Consumer::assignment);
    }

    private ActorRef<BrokerPollSystem<K, V>> consumer() {
        return basePollerRef.getMyActor();
    }

    /**
     * Makes a blocking call to the consumer thread - will return once the other thread has looped over it's control
     * loop. Uses a default timeout of {@link io.confluent.parallelconsumer.internal.DrainingCloseable.DEFAULT_TIMEOUT}
     */
    @SneakyThrows
    @Override
    public Set<String> subscription() {
        return blockingAsk(poller -> poller.getConsumerManager().subscription());
    }

    private <R> R blockingAskConsumer(Function<Consumer<K, V>, R> ask) throws InterruptedException, ExecutionException, TimeoutException {
        return blockingAsk(poller -> ask.apply(poller.getConsumerManager().getConsumer()));
    }

    private <R> R blockingAsk(Function<BrokerPollSystem<K, V>, R> poller) throws InterruptedException, ExecutionException, TimeoutException {
        Future<R> ask = consumer().ask(poller);
        // if the polling system is currently long polling the broker waiting for records, interrupt it to deal with messages
        this.basePollerRef.getConsumerManager().wakeup();
        return ask.get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Makes a blocking call to the consumer thread - will return once the other thread has looped over it's control
     * loop. Uses a default timeout of {@link io.confluent.parallelconsumer.internal.DrainingCloseable.DEFAULT_TIMEOUT}
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        blockingAskConsumerVoid(consumer -> consumer.subscribe(pattern, callback));
    }

    /**
     * Makes a blocking call to the consumer thread - will return once the other thread has looped over it's control
     * loop. Uses a default timeout of {@link io.confluent.parallelconsumer.internal.DrainingCloseable.DEFAULT_TIMEOUT}
     */
    @Override
    public void subscribe(final Pattern pattern) {

    }

    /**
     * Makes a blocking call to the consumer thread - will return once the other thread has looped over it's control
     * loop. Uses a default timeout of {@link io.confluent.parallelconsumer.internal.DrainingCloseable.DEFAULT_TIMEOUT}
     */
    @Override
    public void unsubscribe() {

    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        blockingAskConsumerVoid(consumer -> consumer.seek(partition, offset));
    }

    private void blockingAskConsumerVoid(java.util.function.Consumer<Consumer<K, V>> ask) {
        java.util.function.Consumer<BrokerPollSystem<K, V>> wrap = poller -> ask.accept(poller.getConsumerManager().getConsumer());
        blockingAskVoid(wrap);
    }

//    private <R> void blockingAskConsumer(Function<Consumer<K, V>> ask) {
//        java.util.function.Consumer<BrokerPollSystem<K, V>> wrap = poller -> ask.accept(poller.getConsumerManager().getConsumer());
//        blockingAskVoid(wrap);
//    }

    @SneakyThrows
    private void blockingAskVoid(java.util.function.Consumer<BrokerPollSystem<K, V>> ask) {
        blockingAsk(poller -> {
            ask.accept(poller);
            return Void.class;
        });
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        blockingAskConsumerVoid(consumer -> consumer.seek(partition, offsetAndMetadata));
    }

    @SneakyThrows
    @Override
    public long position(TopicPartition partition) {
        return blockingAskConsumer(consumer -> consumer.position(partition));
    }

    @SneakyThrows
    @Override
    public long position(final TopicPartition partition, final Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.position(partition, timeout));
    }

    @SneakyThrows
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return blockingAskConsumer(consumer -> consumer.committed(partition));
    }

    @SneakyThrows
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.committed(partition, timeout));
    }

    @SneakyThrows
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return blockingAskConsumer(Consumer::metrics);
    }

    @SneakyThrows
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return blockingAskConsumer(consumer -> consumer.partitionsFor(topic));
    }

    @SneakyThrows
    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.partitionsFor(topic, timeout));
    }

    @SneakyThrows
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return blockingAskConsumer(Consumer::listTopics);
    }

    @SneakyThrows
    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.listTopics(timeout));
    }

    @SneakyThrows
    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        return blockingAskConsumer(consumer -> consumer.currentLag(topicPartition));
    }

    @SneakyThrows
    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return blockingAskConsumer(Consumer::groupMetadata);
    }

    @Override
    public void enforceRebalance() {
        throwInvalidCall(); // ?
//        return blockingAskConsumer(consumer -> consumer.enforceRebalance(partition, timeout));
    }

    @Override
    public void close() {
        throwInvalidCall();
    }

    private void throwInvalidCall() throws ParallelConsumerException {
        boolean swallow = false;
        if (swallow) {
            log.trace("Swallowing invalid call to a method");
        } else {
            throw new ParallelConsumerException("Not allowed");
        }
    }

    @Override
    public void close(final Duration timeout) {
        throwInvalidCall();
    }


    @SneakyThrows
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> collection, Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.endOffsets(collection, timeout));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> collection) {
        return blockingAskConsumer(consumer -> consumer.endOffsets(collection));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> collection, Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.beginningOffsets(collection, timeout));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> collection) {
        return blockingAskConsumer(consumer -> consumer.beginningOffsets(collection));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.offsetsForTimes(timestampsToSearch, timeout));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return blockingAskConsumer(consumer -> consumer.offsetsForTimes(timestampsToSearch));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> set, Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.committed(set, timeout));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> set) {
        return blockingAskConsumer(consumer -> consumer.committed(set));
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> collection) {
        blockingAskConsumerVoid(consumer -> consumer.seekToEnd(collection));
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> collection) {
        blockingAskConsumerVoid(consumer -> consumer.seekToBeginning(collection));
    }

    @Override
    public void assign(Collection<TopicPartition> collection) {
        blockingAskConsumerVoid(consumer -> consumer.assign(collection));
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        // dont allow?
        blockingAskConsumerVoid(consumer -> consumer.subscribe(topics, callback));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        // dont allow?
        blockingAskConsumerVoid(consumer -> consumer.subscribe(topics));
    }

    // not allowed

    @Override
    public Set<TopicPartition> paused() {
        throwInvalidCall();
        return UniSets.of();
    }

    // no-ops


    @Override
    public ConsumerRecords poll(final long timeout) {
        throwInvalidCall();
        return ConsumerRecords.empty();
    }

    @Override
    public ConsumerRecords poll(final Duration timeout) {
        throwInvalidCall();
        return ConsumerRecords.empty();
    }

    @Override
    public void commitSync() {
        // delegate to PC's commit call, with explanation in the javadoc that in normal operation including close, that it's not necessary
        throwInvalidCall();
    }

    @Override
    public void commitSync(final Duration timeout) {
        throwInvalidCall();
    }

    @Override
    public void commitAsync() {
        throwInvalidCall();
    }

    @Override
    public void commitAsync(final OffsetCommitCallback callback) {
        throwInvalidCall();
    }

    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        throwInvalidCall();
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        throwInvalidCall();
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        throwInvalidCall();
    }

    @Override
    public void resume(final Collection<TopicPartition> collection) {
        throwInvalidCall();
    }

    @Override
    public void pause(final Collection<TopicPartition> collection) {
        throwInvalidCall();
    }

    @Override
    public void wakeup() {
        throwInvalidCall();
    }
}
