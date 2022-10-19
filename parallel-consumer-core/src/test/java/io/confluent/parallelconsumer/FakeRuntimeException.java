package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.StandardException;

/**
 * Used for testing error handling - easier to identify than a plan exception.
 *
 * @author Antony Stubbs
 */
@StandardException
public class FakeRuntimeException extends RetriableException {
}
