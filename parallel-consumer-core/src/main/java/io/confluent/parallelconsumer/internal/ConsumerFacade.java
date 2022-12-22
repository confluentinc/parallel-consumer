package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.actors.Actor;
import io.confluent.csid.actors.FunctionWithException;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

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
 * The part of the {@link Consumer} API that PC supports.
 * <p>
 * All methods are / must be thread safe.
 *
 * @author Antony Stubbs
 * @see PCConsumerAPI a restrucited Consumer API with only the supported functions.
 */
// todo rename suffix Async (is it must implement consumer, but is an ASYNC implementation)
@SuppressWarnings(
        {"ClassWithTooManyMethods",// ain't my interface
                "ClassCanBeRecord"}
)
@Slf4j
@RequiredArgsConstructor
// todo which package? root or internal?
public class ConsumerFacade<K, V> implements PCConsumerAPI {

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

    /**
     * Makes a blocking call to the consumer thread - will return once the other thread has looped over it's control
     * loop. Uses a default timeout of {@link io.confluent.parallelconsumer.internal.DrainingCloseable.DEFAULT_TIMEOUT}
     */
    @SneakyThrows
    @Override
    public Set<String> subscription() {
        return blockingAsk(poller -> poller.getConsumerManager().subscription());
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

//    private <R> void blockingAskConsumer(Function<Consumer<K, V>> ask) {
//        java.util.function.Consumer<BrokerPollSystem<K, V>> wrap = poller -> ask.accept(poller.getConsumerManager().getConsumer());
//        blockingAskVoid(wrap);
//    }

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

    @SneakyThrows
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection collection, Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.endOffsets(collection, timeout));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection collection) {
        return blockingAskConsumer(consumer -> consumer.endOffsets(collection));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection collection, Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.beginningOffsets(collection, timeout));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection collection) {
        return blockingAskConsumer(consumer -> consumer.beginningOffsets(collection));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map timestampsToSearch, Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.offsetsForTimes(timestampsToSearch, timeout));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map timestampsToSearch) {
        return blockingAskConsumer(consumer -> consumer.offsetsForTimes(timestampsToSearch));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set set, Duration timeout) {
        return blockingAskConsumer(consumer -> consumer.committed(set, timeout));
    }

    @SneakyThrows
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set set) {
        return blockingAskConsumer(consumer -> consumer.committed(set));
    }

    @Override
    public void seekToEnd(Collection collection) {
        blockingAskConsumerVoid(consumer -> consumer.seekToEnd(collection));
    }

    @Override
    public void seekToBeginning(Collection collection) {
        blockingAskConsumerVoid(consumer -> consumer.seekToBeginning(collection));
    }

    @Override
    public void assign(Collection collection) {
        blockingAskConsumerVoid(consumer -> consumer.assign(collection));
    }

    @Override
    public void subscribe(Collection topics, ConsumerRebalanceListener callback) {
        // dont allow?
        blockingAskConsumerVoid(consumer -> consumer.subscribe(topics, callback));
    }

    @Override
    public void subscribe(Collection topics) {
        // dont allow?
        blockingAskConsumerVoid(consumer -> consumer.subscribe(topics));
    }

    @Override
    @SneakyThrows
    public Set<TopicPartition> paused() {
        return blockingAskConsumer(Consumer::paused);

    }

    @Override
    public void commitSync() {
        blockingAskConsumerVoid(Consumer::commitSync);
    }

    @Override
    public void commitSync(Duration timeout) {
        blockingAskConsumerVoid(consumer -> consumer.commitSync(timeout));
    }

    @Override
    public void commitAsync() {
        blockingAskConsumerVoid(Consumer::commitAsync);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        blockingAskConsumerVoid(consumer -> consumer.commitAsync(callback));

    }

    @Override
    public void wakeup() {
        // audit
        blockingAskConsumerVoid(Consumer::wakeup);
    }

    private <R> void blockingAskConsumerVoid(java.util.function.Consumer<Consumer<K, V>> ask) {
        java.util.function.Consumer<BrokerPollSystem<K, V>> wrap = poller -> ask.accept(poller.getConsumerManager().getConsumer());
        blockingAskVoid(wrap);
    }

    @SneakyThrows
    private <R> void blockingAskVoid(java.util.function.Consumer<BrokerPollSystem<K, V>> ask) {
        blockingAsk(poller -> {
            ask.accept(poller);
            return Void.class;
        });
    }

    private <R> R blockingAskConsumer(Function<Consumer<K, V>, R> ask) throws InterruptedException, ExecutionException, TimeoutException {
        return blockingAsk(poller -> ask.apply(poller.getConsumerManager().getConsumer()));
    }

    // todo move to Actor?
    private <R> R blockingAsk(FunctionWithException<BrokerPollSystem<K, V>, R> poller) throws InterruptedException, ExecutionException, TimeoutException {
        Future<R> ask = polllerActor().ask(poller);
        return ask.get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private Actor<BrokerPollSystem<K, V>> polllerActor() {
        return basePollerRef.getMyActor();
    }

}
