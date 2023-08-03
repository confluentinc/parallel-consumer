package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import java.io.Closeable;
import java.time.Duration;

import io.confluent.parallelconsumer.ParallelConsumerOptions;

public interface DrainingCloseable extends Closeable {

    enum DrainingMode {
        /**
         * Stop downloading more messages from the Broker, but finish processing what has already been queued.
         */
        DRAIN,
        /**
         * Stop downloading more messages, and stop processing more messages in the queue, but finish processing
         * messages already being processed locally.
         */
        DONT_DRAIN
    }

    /**
     * Close the consumer, without draining. Uses a timeout specified through ParallelConsumerOptions.
     *
     * @see ParallelConsumerOptions#shutdownTimeout
     * @see #close(Duration, DrainingMode)
     */
    default void close() {
        closeDontDrainFirst();
    }

    /**
     * @see DrainingMode#DRAIN
     */
    default void closeDrainFirst() {
        close(DrainingMode.DRAIN);
    }

    /**
     * @see DrainingMode#DONT_DRAIN
     */
    default void closeDontDrainFirst() {
        close(DrainingMode.DONT_DRAIN);
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
     * @param timeout      how long to wait before giving up - override timeout set in ParallelConsumerOptions
     * @param drainingMode specify if PC should wait for messages already consumed from the broker to be processed before closing
     */
    void close(Duration timeout, DrainingMode drainingMode);

    /**
     * Close the consumer using timeout specified in ParallelConsumerOptions
     *
     * @param drainingMode wait for messages already consumed from the broker to be processed before closing
     */
    void close(DrainingMode drainingMode);

    /**
     * Of the records consumed from the broker, how many do we have remaining in our local queues
     *
     * @return the number of consumed but outstanding records to process
     */
    long workRemaining();

}
