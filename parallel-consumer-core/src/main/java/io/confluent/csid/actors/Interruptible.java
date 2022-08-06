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
//        /**
//         * @deprecated should not be used, as bypasses the requirement of providing a reason
//         */
//        @Deprecated
//        public static final Reason UNKNOWN = new Reason("unknown-reason");

        /**
         * The description of the cause or need for interruption.
         */
        private final String desc;

        /**
         * Sometimes there is a root cause (Reason) for the Reason to interrupt something.
         */
        // todo not used, removed
        @Deprecated
        private Reason root;
    }

}
