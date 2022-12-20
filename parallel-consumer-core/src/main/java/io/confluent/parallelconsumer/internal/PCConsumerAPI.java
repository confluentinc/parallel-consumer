package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

/**
 * The subset of functions accessible from the underlying {@link Consumer}. Naturally, cannot implement the [@link
 * Consumer} interface.
 * <p>
 * This exposes a limited subset of the {@link Consumer} interface that is valid to be used within the PC context.
 * <p>
 * Generally, you can only... And you can't...
 * <p>
 * All methods are / must be thread safe.
 *
 * @param <K>
 * @param <V>
 * @author Antony Stubbs
 */
public interface PCConsumerAPI<K, V> {

    @SneakyThrows
    Set<TopicPartition> assignment();

    @SneakyThrows
    Set<String> subscription();

    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    void subscribe(Pattern pattern);

    void unsubscribe();

    void seek(TopicPartition partition, long offset);

    void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

    @SneakyThrows
    long position(TopicPartition partition);

    @SneakyThrows
    long position(TopicPartition partition, Duration timeout);

    @SneakyThrows
    OffsetAndMetadata committed(TopicPartition partition);

    @SneakyThrows
    OffsetAndMetadata committed(TopicPartition partition, Duration timeout);

    @SneakyThrows
    Map<MetricName, ? extends Metric> metrics();

    @SneakyThrows
    List<PartitionInfo> partitionsFor(String topic);

    @SneakyThrows
    List<PartitionInfo> partitionsFor(String topic, Duration timeout);

    @SneakyThrows
    Map<String, List<PartitionInfo>> listTopics();

    @SneakyThrows
    Map<String, List<PartitionInfo>> listTopics(Duration timeout);

    @SneakyThrows
    OptionalLong currentLag(TopicPartition topicPartition);

    @SneakyThrows
    ConsumerGroupMetadata groupMetadata();
//
//    /**
//     * Closes the entire PC system, not just the consumer.
//     *
//     * @see ParallelEoSStreamProcessor#close
//     */
//    void close();
//
//    /**
//     * Closes the entire PC system, not just the consumer.
//     *
//     * @see ParallelEoSStreamProcessor#close
//     */
//    void close(Duration timeout);

    @SneakyThrows
    Map<TopicPartition, Long> endOffsets(Collection collection, Duration timeout);

    @SneakyThrows
    Map<TopicPartition, Long> endOffsets(Collection collection);

    @SneakyThrows
    Map<TopicPartition, Long> beginningOffsets(Collection collection, Duration timeout);

    @SneakyThrows
    Map<TopicPartition, Long> beginningOffsets(Collection collection);

    @SneakyThrows
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map timestampsToSearch, Duration timeout);

    @SneakyThrows
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map timestampsToSearch);

    @SneakyThrows
    Map<TopicPartition, OffsetAndMetadata> committed(Set set, Duration timeout);

    @SneakyThrows
    Map<TopicPartition, OffsetAndMetadata> committed(Set set);

    void seekToEnd(Collection collection);

    void seekToBeginning(Collection collection);

    void assign(Collection collection);

    void subscribe(Collection topics, ConsumerRebalanceListener callback);

    void subscribe(Collection topics);

    Set<TopicPartition> paused();

    void commitSync();

    void commitSync(Duration timeout);

    void commitAsync();

    void commitAsync(OffsetCommitCallback callback);

    void wakeup();
}
