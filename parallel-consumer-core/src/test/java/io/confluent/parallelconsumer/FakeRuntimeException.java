package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

/**
 * Used for testing error handling - easier to identify than a plan exception.
 */
public class FakeRuntimeException extends RuntimeException {
    public FakeRuntimeException(String msg) {
        super(msg);
    }
}
