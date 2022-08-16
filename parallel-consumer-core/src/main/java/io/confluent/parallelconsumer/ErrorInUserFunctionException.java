package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.StandardException;

/**
 * This exception is only used when there is an exception thrown from code provided by the user.
 */
@StandardException
public class ErrorInUserFunctionException extends ParallelConsumerException {
}
