package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkContainer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

import java.util.Optional;

/**
 * Internal thread safe API for the Controller
 *
 * @author Antony Stubbs
 */
//todo merge with ControllerPackageAPI?
//todo extract the other interfaces
public interface ControllerInternalAPI<K, V> extends ThreadSafeAPI, ConsumerRebalanceListener {

    /**
     * todo docs
     */
    @ThreadSafe
    void sendWorkResultAsync(PollContextInternal<K, V> pollContext, WorkContainer<K, V> wc);

    /**
     * todo docs
     */
    @ThreadSafe
    // make sense to annotate the interface?
    void sendNewPolledRecordsAsync(EpochAndRecordsMap<K, V> polledRecords);

    /**
     * todo docs
     */
    Optional<String> getMyId();

}
