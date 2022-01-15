package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
import java.io.Closeable;
import java.time.Duration;

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
     * Close the consumer, without draining. Uses a reasonable default timeout.
     *
     * @see #DEFAULT_TIMEOUT
     * @see #close(Duration, DrainingMode)
     */
    default void close() {
        closeDontDrainFirst();
    }

    /**
     * @see DrainingMode#DRAIN
     */
    default void closeDrainFirst() {
        closeDrainFirst(DEFAULT_TIMEOUT);
    }

    /**
     * @see DrainingMode#DONT_DRAIN
     */
    default void closeDontDrainFirst() {
        closeDontDrainFirst(DEFAULT_TIMEOUT);
    }

    /**
     * @see DrainingMode#DRAIN
     */
    default void closeDrainFirst(Duration timeout) {
        close(timeout, DrainingMode.DRAIN);
    }

    /**
     * @see DrainingMode#DONT_DRAIN
     */
    default void closeDontDrainFirst(Duration timeout) {
        close(timeout, DrainingMode.DONT_DRAIN);
    }

    /**
     * Close the consumer.
     *
     * @param timeout      how long to wait before giving up
     * @param drainingMode wait for messages already consumed from the broker to be processed before closing
     */
    void close(Duration timeout, DrainingMode drainingMode);

    /**
     * Of the records consumed from the broker, how many do we have remaining in our local queues
     *
     * @return the number of consumed but outstanding records to process
     */
    long workRemaining();

}
