package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.StandardException;

/**
 * Generic internal runtime error
 */
@StandardException
public class InternalRuntimeError extends RuntimeException {
}
