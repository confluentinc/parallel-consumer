package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashSet;

/**
 * Separate out concerns from the Controller, and so the user doesn't have access to the public rebalance interface
 * methods.
 * <p>
 * Note: Is mainly an abstract class instead of an Interface, as you can't have protected methods in interfaces (they
 * must all be public)
 *
 * @author Antony Stubbs
 */
// todo partial refactor - continued on controller refactor branch
@RequiredArgsConstructor
public abstract class RebalanceHandler implements ConsumerRebalanceListener {

    private final Consumer<?, ?> consumer;

    @ThreadSafe
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        onPartitionsRevokedTellAsync(partitions);
    }

    @ThreadSafe
    protected abstract void onPartitionsRevokedTellAsync(Collection<TopicPartition> partitions);

    @ThreadSafe
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        var raw = consumer.committed(new HashSet<>(partitions));
        var pastCommitData = new CommitData(raw);
        onPartitionsAssignedTellAsync(pastCommitData);
    }

//    private Consumer<?, ?> consumer() {
//        return consumer;
//    }

    @ThreadSafe
    protected abstract void onPartitionsAssignedTellAsync(CommitData partitions);

}
