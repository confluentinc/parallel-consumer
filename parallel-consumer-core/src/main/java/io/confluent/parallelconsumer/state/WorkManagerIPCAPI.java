package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.ThreadSafeAPI;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

/**
 * @author Antony Stubbs
 */
public interface WorkManagerIPCAPI<K, V> extends ConsumerRebalanceListener, ThreadSafeAPI {

    /**
     * todo docs
     * <p>
     * todo should be removed
     */
    PartitionStateManager<K, V> getPm();

    /**
     * todo docs
     */
    boolean shouldThrottle();
}
