package io.confluent.parallelconsumer;


/**
 * Generic Parallel Consumer {@link RuntimeException} parent.
 */
public class ParallelConsumerException extends RuntimeException {
    public ParallelConsumerException(String message, Throwable cause) {
        super(message, cause);
    }
}
