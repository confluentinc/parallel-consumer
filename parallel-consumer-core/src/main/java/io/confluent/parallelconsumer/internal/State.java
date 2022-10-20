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
     * When paused, the system will stop submitting work to the processing pool. Polling for new work however may
     * continue until internal buffers have been filled sufficiently and the auto-throttling takes effect.
     * In flight work will not be affected by transitioning to this state (i.e. processing will finish without any
     * interrupts being sent).
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
