package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

/**
 * Generic Parallel Consumer parent exception.
 */
public class ParallelConsumerInternalException extends Exception {
    public ParallelConsumerInternalException(String msg) {
        super(msg);
    }
}
