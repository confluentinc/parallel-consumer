package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


/**
 * Generic Parallel Consumer {@link RuntimeException} parent.
 */
public class ParallelConsumerException extends RuntimeException {
    public ParallelConsumerException(String message, Throwable cause) {
        super(message, cause);
    }
}
