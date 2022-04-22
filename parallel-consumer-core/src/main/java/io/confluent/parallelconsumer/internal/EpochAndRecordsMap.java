package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.PartitionStateManager;
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
            Long epochOfPartition = pm.getEpochOfPartition(partition);
            recordMap.put(partition, new RecordsAndEpoch(epochOfPartition, records));
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
        int count = 0;
        for (var recs : this.recordMap.values())
            count += recs.getRecords().size();
        return count;
    }

    @Value
    public class RecordsAndEpoch {
        Long epochOfPartitionAtPoll;
        List<ConsumerRecord<K, V>> records;
    }

}
