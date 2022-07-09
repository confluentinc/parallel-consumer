package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;


// todo inline into controller
@RequiredArgsConstructor
public class ConsumerRebalanceHandler<K, V> implements ConsumerRebalanceListener {

    private final AbstractParallelEoSStreamProcessor<K, V> baseController;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        baseController.onPartitionsRevokedTellAsync(partitions);
//        controller.sendPartitionEvent(REVOKED, partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        baseController.onPartitionsAssignedTellAsync(partitions);
//        controller.sendPartitionEvent(ASSIGNED, partitions);
    }

//    enum PartitionEventType {
//        ASSIGNED, REVOKED
//    }

//    @Value
//    protected static class PartitionEventMessage {
//        PartitionEventType type;
//        Collection<TopicPartition> partitions;
//    }
}
