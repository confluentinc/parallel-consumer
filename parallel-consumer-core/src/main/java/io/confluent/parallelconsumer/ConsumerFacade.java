package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.ActorRef;
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.ConsumerRebalanceHandler;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
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
 */
// todo rename suffix Async (is it must implement consumer, but is an ASYNC implementation)
@Slf4j
@RequiredArgsConstructor
public class ConsumerFacade<K, V> implements Consumer<K, V> {

    static class Bus {
        Queue<ConsumerRebalanceHandler.Message> queue;
        Exchanger<ConsumerRebalanceHandler.Message> exchanger;
        LinkedTransferQueue<ConsumerRebalanceHandler.Message> transferQueue;

        public void blockingEnqueue(SeekMessage seekMessage) {
//            queue.add(seekMessage);
//            exchanger.exchange(seekMessage);
//            transferQueue.transfer(seekMessage);
        }

//        public SeekMessage poll() {
////            exchanger.
//        }
    }

    private final Bus bus;
    private final AbstractParallelEoSStreamProcessor<?, ?> controller;
    private final BrokerPollSystem<?, ?> basePollerRef;

    /**
     * Makes a blocking call to the consumer thread - will return once the other thread has looped over it's control
     * loop. Uses a default timeout of {@link io.confluent.parallelconsumer.internal.DrainingCloseable.DEFAULT_TIMEOUT}
     */
    /// above
    @SneakyThrows
    @Override
    public Set<TopicPartition> assignment() {
//        controller.getWm().getPm().assignment();
        Future<Set<TopicPartition>> ask = consumer().ask(poller -> poller.getConsumerManager().assignment());
        return ask.get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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
        return blockingAsk((poller) -> poller.getConsumerManager().subscription());
    }

    private Set blockingAsk(final Function<BrokerPollSystem, Set> brokerPollSystemSetFunction) throws InterruptedException, ExecutionException, TimeoutException {
        Future<Set> ask = consumer().ask(brokerPollSystemSetFunction);
        return ask.get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Makes a blocking call to the consumer thread - will return once the other thread has looped over it's control
     * loop. Uses a default timeout of {@link io.confluent.parallelconsumer.internal.DrainingCloseable.DEFAULT_TIMEOUT}
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        Future<Set> ask = consumer().ask((poller) -> poller.getConsumerManager().subscribe(pattern, callback));
        return ask.get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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


    @Value
    @EqualsAndHashCode(callSuper = true)
    static class SeekMessage extends ConsumerRebalanceHandler.Message {
        TopicPartition partition;
        long offset;
    }

    @Override
    public void seek(final TopicPartition partition, final long offset) {
        bus.blockingEnqueue(new SeekMessage(partition, offset));
    }

    @Override
    public void seek(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {

    }

    @Override
    public long position(final TopicPartition partition) {
        return 0;
    }

    @Override
    public long position(final TopicPartition partition, final Duration timeout) {
        return 0;
    }

    @Override
    public OffsetAndMetadata committed(final TopicPartition partition) {
        return null;
    }

    @Override
    public OffsetAndMetadata committed(final TopicPartition partition, final Duration timeout) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic) {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic, final Duration timeout) {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(final Duration timeout) {
        return null;
    }

    @Override
    public Set<TopicPartition> paused() {
        return null;
    }


    @Override
    public OptionalLong currentLag(final TopicPartition topicPartition) {
        return null;
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return null;
    }

    @Override
    public void enforceRebalance() {

    }

    @Override
    public void close() {

    }

    @Override
    public void close(final Duration timeout) {

    }


    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection collection, final Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection collection) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection collection, final Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection collection) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map timestampsToSearch, final Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map timestampsToSearch) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set set, final Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set set) {
        return null;
    }

    @Override
    public void seekToEnd(final Collection collection) {

    }

    @Override
    public void seekToBeginning(final Collection collection) {

    }

    @Override
    public void assign(final Collection collection) {

    }

    @Override
    public void subscribe(final Collection topics, final ConsumerRebalanceListener callback) {

    }

    @Override
    public void subscribe(final Collection topics) {

    }

    // no-ops


    @Override
    public ConsumerRecords poll(final long timeout) {
        return null;
    }

    @Override
    public ConsumerRecords poll(final Duration timeout) {
        return null;
    }

    @Override
    public void commitSync() {

    }

    @Override
    public void commitSync(final Duration timeout) {

    }

    @Override
    public void commitAsync() {

    }

    @Override
    public void commitAsync(final OffsetCommitCallback callback) {

    }

    @Override
    public void commitAsync(final Map offsets, final OffsetCommitCallback callback) {

    }

    @Override
    public void commitSync(final Map offsets, final Duration timeout) {

    }

    @Override
    public void commitSync(final Map offsets) {

    }

    @Override
    public void resume(final Collection collection) {

    }

    @Override
    public void pause(final Collection collection) {

    }

    @Override
    public void wakeup() {

    }
}
