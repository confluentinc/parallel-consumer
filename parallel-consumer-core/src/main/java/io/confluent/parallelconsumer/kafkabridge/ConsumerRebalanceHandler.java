package io.confluent.parallelconsumer.kafkabridge;

import io.confluent.parallelconsumer.controller.AbstractParallelEoSStreamProcessor;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

import static io.confluent.parallelconsumer.kafkabridge.ConsumerRebalanceHandler.PartitionEventType.ASSIGNED;
import static io.confluent.parallelconsumer.kafkabridge.ConsumerRebalanceHandler.PartitionEventType.REVOKED;

@RequiredArgsConstructor
public class ConsumerRebalanceHandler<K, V> implements ConsumerRebalanceListener {

    AbstractParallelEoSStreamProcessor<K, V> controller;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        controller.sendPartitionEvent(REVOKED, partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        controller.sendPartitionEvent(ASSIGNED, partitions);
    }

    public enum PartitionEventType {
        ASSIGNED, REVOKED
    }

    @Value
    public static class PartitionEventMessage {
        PartitionEventType type;
        Collection<TopicPartition> partitions;
    }
}
