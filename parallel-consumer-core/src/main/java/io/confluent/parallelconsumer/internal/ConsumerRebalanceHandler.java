package io.confluent.parallelconsumer.internal;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Queue;

import static io.confluent.parallelconsumer.internal.ConsumerRebalanceHandler.PartitionEventType.ASSIGNED;
import static io.confluent.parallelconsumer.internal.ConsumerRebalanceHandler.PartitionEventType.REVOKED;

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

    enum PartitionEventType {
        ASSIGNED, REVOKED
    }

    public static class Message {
        Queue<Message> reponseQueue;

        public void reply(Message response) {
            reponseQueue.add(response);
        }
    }

    @Value
    protected static class PartitionEventMessage extends Message {
        PartitionEventType type;
        Collection<TopicPartition> partitions;
    }
}
