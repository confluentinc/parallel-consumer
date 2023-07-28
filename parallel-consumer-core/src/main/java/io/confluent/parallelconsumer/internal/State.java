package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import lombok.Getter;

import java.util.function.Predicate;

/**
 * The run state of the controller.
 */
public enum State {
    UNUSED(0),
    RUNNING(1),
    /**
     * When paused, the system will stop submitting work to the processing pool. Polling for new work however may
     * continue until internal buffers have been filled sufficiently and the auto-throttling takes effect. In flight
     * work will not be affected by transitioning to this state (i.e. processing will finish without any interrupts
     * being sent).
     */
    PAUSED(2),
    /**
     * When draining, the system will stop polling for more records, but will attempt to process all already downloaded
     * records. Note that if you choose to close without draining, records already processed will still be committed
     * first before closing.
     */
    DRAINING(3),
    CLOSING(4),
    CLOSED(5);

    // Enum value used for metrics - deterministic as opposed to ordinal to prevent change on adding / removing enum constants
    @Getter
    private int value;

    State(int value) {
        this.value = value;
    }
}
