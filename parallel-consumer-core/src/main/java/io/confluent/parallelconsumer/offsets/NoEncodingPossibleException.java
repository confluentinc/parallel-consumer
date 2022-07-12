package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.ParallelConsumerInternalException;
import lombok.experimental.StandardException;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@StandardException

public class NoEncodingPossibleException extends ParallelConsumerInternalException {

    public NoEncodingPossibleException(String msg) {
        super(msg);
    }
}
