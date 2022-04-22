package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

/**
 * The run state of the controller.
 */
public enum RunState {
    unused,
    running,
    /**
     * When draining, the system will stop polling for more records, but will attempt to process all already downloaded
     * records. Note that if you choose to close without draining, records already processed will still be committed
     * first before closing.
     */
    draining,
    closing,
    closed
}
