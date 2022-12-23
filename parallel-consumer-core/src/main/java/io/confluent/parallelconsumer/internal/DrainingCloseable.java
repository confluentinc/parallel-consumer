package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.SneakyThrows;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * @author Antony Stubbs
 */
public interface DrainingCloseable extends Closeable {

    Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30); // can increase if debugging

    enum DrainingMode {
        /**
         * Stop downloading more messages from the Broker, but finish processing what has already been queued.
         */
        DRAIN,
        /**
         * Stop downloading more messages, and stop procesing more messages in the queue, but finish processing messages
         * already being processed locally.
         */
        DONT_DRAIN
    }

    /**
     * Close the consumer, without draining. Uses a reasonable default timeout. Blocking until it has finished closing.
     *
     * @see #DEFAULT_TIMEOUT
     * @see #close(Duration, DrainingMode)
     */
    @SneakyThrows
    default void close() {
        closeDontDrainFirst();
    }

    /**
     * Close the consumer, blocking until it has finished closing.
     *
     * @see DrainingMode#DRAIN
     */
    default void closeDrainFirst() throws ExecutionException, InterruptedException, TimeoutException {
        closeDrainFirst(DEFAULT_TIMEOUT);
    }

    /**
     * Close the consumer, blocking until it has finished closing.
     *
     * @see DrainingMode#DONT_DRAIN
     */
    default void closeDontDrainFirst() throws ExecutionException, InterruptedException, TimeoutException {
        closeDontDrainFirst(DEFAULT_TIMEOUT);
    }

    /**
     * Close the consumer, blocking until it has finished closing.
     *
     * @see DrainingMode#DRAIN
     */
    default void closeDrainFirst(Duration timeout) throws ExecutionException, InterruptedException, TimeoutException {
        close(timeout, DrainingMode.DRAIN);
    }

    /**
     * Close the consumer, blocking until it has finished closing.
     *
     * @see DrainingMode#DONT_DRAIN
     */
    default void closeDontDrainFirst(Duration timeout) throws ExecutionException, InterruptedException, TimeoutException {
        close(timeout, DrainingMode.DONT_DRAIN);
    }

    /**
     * Close the consumer, blocking until it has finished closing.
     *
     * @param timeout      how long to wait before giving up
     * @param drainingMode wait for messages already consumed from the broker to be processed before closing
     */
    // todo consider - do users want to have checked or unchecked exceptions when closing?
    void close(Duration timeout, DrainingMode drainingMode) throws ExecutionException, InterruptedException, TimeoutException;

    /**
     * Of the records consumed from the broker, how many do we have remaining in our local queues
     *
     * @return the number of consumed but outstanding records to process
     */
    // todo move - doesn't belong here?
    long workRemaining();

}
