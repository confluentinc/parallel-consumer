package io.confluent.parallelconsumer.internal;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * Separate out concerns from the Controller, and so the user doesn't have access to the public rebalance interface
 * methods.
 *
 * @author Antony Stubbs
 */
// todo partial refactor - continued on controller refactor branch
@RequiredArgsConstructor
public abstract class RebalanceHandler implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        onPartitionsRevokedTellAsync(partitions);
    }

    protected abstract void onPartitionsRevokedTellAsync(Collection<TopicPartition> partitions);

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        onPartitionsAssignedTellAsync(partitions);
    }

    protected abstract void onPartitionsAssignedTellAsync(Collection<TopicPartition> partitions);

}
