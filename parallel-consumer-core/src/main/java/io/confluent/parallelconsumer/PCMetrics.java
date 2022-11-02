package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.State;
import io.confluent.parallelconsumer.offsets.OffsetEncoding;
import io.confluent.parallelconsumer.state.ShardKey;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Optional;

/**
 * Metrics model for Parallel Consumer
 *
 * @author Antony Stubbs
 */
@Value
@SuperBuilder(toBuilder = true)
public class PCMetrics {

    long dynamicLoadFactor;
    long incrementingCountOfRecordsProcessed;

    Map<TopicPartition, PCPartitionMetrics> partitionMetrics;

    Map<ShardKey, ShardMetrics> shardMetrics;

    PollerMetrics pollerMetrics;

    Timer functionTimer;

    Counter successCounter;

    /**
     * The number of partitions assigned to this consumer
     */
    public long getNumberOfPartitions() {
        return partitionMetrics.size();
    }

    /**
     * The number of shards (queues) currently managed - depends on ordering and data key set
     */
    public long getNumberOfShards() {
        return shardMetrics.size();
    }

    public long getTotalNumberOfIncompletes() {
        return partitionMetrics.values().stream()
                .mapToLong(PCPartitionMetrics::getNumberOfIncompletes)
                .sum();
    }

    @Value
    @SuperBuilder(toBuilder = true)
    public static class PCPartitionMetrics {
        TopicPartition topicPartition;
        long lastCommittedOffset;
        long numberOfIncompletes;
        long highestCompletedOffset;
        long highestSeenOffset;
        long highestSequentialSucceededOffset;
        long epoch;
        CompressionStats compressionStats;

        /**
         * @see AbstractParallelEoSStreamProcessor#calculateMetricsWithIncompletes()
         */
        @Builder.Default
        Optional<IncompleteMetrics> incompleteMetrics = Optional.empty();

        public long getTraditionalConsumerLag() {
            return highestSeenOffset - highestSequentialSucceededOffset;
        }

        @Value
        public static class IncompleteMetrics {
            Offsets incompleteOffsets;
        }

        @Value
        @SuperBuilder(toBuilder = true)
        public static class CompressionStats {
            long offsetsEncodedPerBit;
            long offsetsEncodedPerByte;
            long bytesUsedForEncoding;
            double fractionOfEncodingSpaceUsed;
            OffsetEncoding bestEncoding;
        }
    }

    @Value
    @SuperBuilder(toBuilder = true)
    public static class ShardMetrics {
        ShardKey shardKey;
        /**
         * Number of records queued for processing in this shard
         */
        long shardSize;
        long averageUserProcessingTime;
        long averageTimeSpentInQueue;
    }

    /**
     * Metrics for the {@link io.confluent.parallelconsumer.internal.BrokerPollSystem} sub-system.
     */
    @Value
    @SuperBuilder(toBuilder = true)
    public static class PollerMetrics {
        State state;
        boolean paused;
        Map<TopicPartition, Boolean> pausedPartitions;
    }

}
