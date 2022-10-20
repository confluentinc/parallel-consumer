package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.StandardException;

/**
 * Generic Parallel Consumer {@link RuntimeException} parent.
 *
 * @author Antony Stubbs
 */
@StandardException
public class ParallelConsumerException extends RuntimeException {
}
