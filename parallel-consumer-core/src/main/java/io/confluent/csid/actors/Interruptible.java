package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Allows something to be interrupted with a {@link Reason}.
 *
 * @author Antony Stubbs
 */
// todo removable after actors fully merged
public interface Interruptible {

    /**
     * A simple convenience method to push an effectively NO-OP message, which would wake it up if it were blocked.
     * Useful to have a blocked thread return from the process method if it's blocked, without needed to
     * {@link Thread#interrupt} it, but you don't want to or can't send it through closure for some reason.
     *
     * @param reason the reason for interrupting the Actor
     * @deprecated rather than call this generic wakeup method, it's better to send a message directly to your Actor, so
     *         that the interrupt has context. However, this can be useful to use for legacy code.
     */
    @Deprecated
    void interrupt(Reason reason);

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
        @Deprecated
        private Reason root;
    }

}
