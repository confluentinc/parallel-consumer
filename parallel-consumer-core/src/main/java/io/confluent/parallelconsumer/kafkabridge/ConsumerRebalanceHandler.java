package io.confluent.parallelconsumer.kafkabridge;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.ControllerEventBus;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

import static io.confluent.parallelconsumer.sharedstate.PartitionEventMessage.PartitionEventType.ASSIGNED;
import static io.confluent.parallelconsumer.sharedstate.PartitionEventMessage.PartitionEventType.REVOKED;

@RequiredArgsConstructor
public class ConsumerRebalanceHandler<K, V> implements ConsumerRebalanceListener {

    ControllerEventBus<K, V> controller;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        controller.sendPartitionEvent(REVOKED, partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        controller.sendPartitionEvent(ASSIGNED, partitions);
    }

}
