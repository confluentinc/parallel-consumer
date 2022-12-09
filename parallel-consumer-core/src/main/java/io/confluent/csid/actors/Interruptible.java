package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Something which is able to be interrupted, with a {@link Reason}.
 *
 * @author Antony Stubbs
 */
public interface Interruptible {

    /**
     * If blocked waiting on messages, this will interrupt that wait.
     */
    // todo rename
    void interruptMaybePollingActor(Reason reason);

    /**
     * Structured enforcement of Human readable reasons for interrupting something.
     */
    @AllArgsConstructor
    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode
    class Reason {

        /**
         * The description of the cause or need for interruption.
         */
        private final String desc;

        /**
         * Sometimes there is a root cause (Reason) for the Reason to interrupt something.
         */
        // todo this can be used to carry exceptions?
        // todo not used, removed
        @Deprecated
        private Reason root;
    }

}
