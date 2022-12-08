package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

/**
 * Package level thread safe API for the Controller.
 * <p>
 * Not user facing.
 *
 * @author Antony Stubbs
 */
//todo extract the other interfaces
public interface ControllerPackageAPI<K, V> extends ThreadSafeAPI, ConsumerRebalanceListener {

    @ThreadSafe
        // make sense to annotate the interface?
    void sendNewPolledRecordsAsync(EpochAndRecordsMap<K, V> polledRecords);

//    Optional<Object> getMyId();

}
