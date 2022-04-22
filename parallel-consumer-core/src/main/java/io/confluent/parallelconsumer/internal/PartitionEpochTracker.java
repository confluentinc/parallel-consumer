package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PartitionEpochTracker {

    /**
     * Record the generations of partition assignment, for fencing off invalid work.
     * <p>
     * NOTE: This must live outside of {@link PartitionState}, as it must be tracked across partition lifecycles.
     * <p>
     * Starts at zero.
     */
    private final Map<TopicPartition, Long> partitionsAssignmentEpochs = new HashMap<>();

    public void incrementPartitionAssignmentEpoch(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            Long epoch = partitionsAssignmentEpochs.getOrDefault(partition, ParallelConsumerOptions.KAFKA_OFFSET_ABSENCE);
            epoch++;
            partitionsAssignmentEpochs.put(partition, epoch);
        }
    }

    public Long getEpochOfPartition(TopicPartition partition) {
        return partitionsAssignmentEpochs.get(partition);
    }

    public Long getEpochFor(TopicPartition tp) {
        return partitionsAssignmentEpochs.get(tp);
    }
}
