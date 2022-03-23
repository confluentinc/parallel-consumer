package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.ParallelConsumerInternalException;

public class NoEncodingPossibleException extends ParallelConsumerInternalException {

    public NoEncodingPossibleException(String msg) {
        super(msg);
    }
}
