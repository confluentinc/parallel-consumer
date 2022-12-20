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
public interface ControllerInternalAPI<K, V> extends IThreadSafeAPI, ConsumerRebalanceListener {

    /**
     * Thread safe async sending of results from worker thread
     */
    @ThreadSafe
    void sendWorkResultAsync(PollContextInternal<K, V> pollContext, WorkContainer<K, V> wc);

    /**
     * Thread safe async sending of newly polled records
     */
    @ThreadSafe
    // make sense to annotate the interface?
    void sendNewPolledRecordsAsync(EpochAndRecordsMap<K, V> polledRecords);

    /**
     * The configured ID of the parallel consumer instance
     * <p>
     * todo @see ParallelConsumerOptions after controller refactor
     */
    Optional<String> getMyId();

}
