package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

/**
 * The run state of the controller.
 */
public enum State {
    unused,
    running,
    /**
     * When paused, the system will stop polling for new records from the broker and also stop submitting work that has already
     * been polled to the processing pool.
     *  Committing offsets however
     * Already submitted in flight work however will be finished (i.e. ).
     * Parallel stream processor handleWork
     */
    paused,
    /**
     * When draining, the system will stop polling for more records, but will attempt to process all already downloaded
     * records. Note that if you choose to close without draining, records already processed will still be committed
     * first before closing.
     */
    draining,
    closing,
    closed;
}
