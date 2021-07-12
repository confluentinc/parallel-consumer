package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

/**
 * This exception is only used when there is an exception thrown from code provided by the user.
 */
public class ErrorInUserFunctionException extends RuntimeException {
    public ErrorInUserFunctionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
