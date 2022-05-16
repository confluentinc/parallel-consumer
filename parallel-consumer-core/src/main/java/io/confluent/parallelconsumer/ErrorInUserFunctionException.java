package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

/**
 * This exception is only used when there is an exception thrown from code provided by the user.
 */
public class ErrorInUserFunctionException extends ParallelConsumerException {
    public ErrorInUserFunctionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
