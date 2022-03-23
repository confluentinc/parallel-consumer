package io.confluent.parallelconsumer.internal;

/**
 * Generic Parallel Consumer parent exception.
 */
public class ParallelConsumerInternalException extends Exception {
    public ParallelConsumerInternalException(String msg) {
        super(msg);
    }
}
