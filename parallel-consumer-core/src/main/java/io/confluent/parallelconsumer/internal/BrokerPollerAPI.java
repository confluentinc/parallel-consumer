package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public interface BrokerPollerAPI extends OffsetCommitter, Supervisable, MultithreadingAPI {

    /**
     * todo docs
     */
    @ThreadSafe
    void start(String managedExecutorService);

    /**
     * todo docs
     */
    @ThreadSafe
    boolean isPausedForThrottling();

    /**
     * todo docs
     */
    @ThreadSafe
    void wakeupIfPaused();

    /**
     * todo docs
     */
    // todo move to DrainingCloseable?
    @ThreadSafe
    void drain();

    /**
     * todo docs
     */
    @ThreadSafe
    // todo move to or rename, to be in/from DrainingCloseable
    void closeAndWait() throws TimeoutException, ExecutionException;
}
