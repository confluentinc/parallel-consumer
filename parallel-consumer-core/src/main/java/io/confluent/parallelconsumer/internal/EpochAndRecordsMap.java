package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.PartitionStateManager;
import lombok.NonNull;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * For tagging polled records with our epoch
 *
 * @see BrokerPollSystem#partitionAssignmentEpoch
 */
@Value
public class EpochAndRecordsMap<K, V> {

    Map<TopicPartition, RecordsAndEpoch> recordMap = new HashMap<>();

    public EpochAndRecordsMap(ConsumerRecords<K, V> poll, PartitionStateManager<K, V> pm) {
        poll.partitions().forEach(partition -> {
            var records = poll.records(partition);
            var epochOfPartition = pm.getEpochOfPartition(partition);
            RecordsAndEpoch entry = new RecordsAndEpoch(partition, epochOfPartition, records);
            recordMap.put(partition, entry);
        });
    }

    /**
     * Get the partitions which have records contained in this record set.
     *
     * @return the set of partitions with data in this record set (may be empty if no data was returned)
     */
    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(recordMap.keySet());
    }

    /**
     * Get just the records for the given partition
     *
     * @param partition The partition to get records for
     */
    public RecordsAndEpoch records(TopicPartition partition) {
        return this.recordMap.get(partition);
    }

    /**
     * The number of records for all topics
     */
    public int count() {
        return this.recordMap.values().stream()
                .mapToInt(x ->
                        x.getRecords().size()
                )
                .sum();
    }

    @Value
    public class RecordsAndEpoch {

        @NonNull TopicPartition topicPartition;

        @NonNull Long epochOfPartitionAtPoll;

        @NonNull List<ConsumerRecord<K, V>> records;
    }

    @Value
    private class PartitionEpoch {

        @NonNull Long epoch;
    }
}
