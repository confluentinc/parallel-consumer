package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Queue;

import static io.confluent.parallelconsumer.internal.ConsumerRebalanceHandler.PartitionEventType.REVOKED;

@RequiredArgsConstructor
public class ConsumerRebalanceHandler<K, V> implements ConsumerRebalanceListener {

    AbstractParallelEoSStreamProcessor<K, V> baseController;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        baseController.sendPartitionEvent(REVOKED, partitions);

        //
        baseController.getMyActor()
                .tell(controller -> controller.onPartitionsRevoked(partitions));
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//        baseController.sendPartitionEvent(ASSIGNED, partitions);

        baseController.getMyActor()
                .tell(controller -> controller.onPartitionsAssigned(partitions));
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
