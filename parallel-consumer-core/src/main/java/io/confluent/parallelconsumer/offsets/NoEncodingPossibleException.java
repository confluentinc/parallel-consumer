package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalException;

public class NoEncodingPossibleException extends InternalException {

    public NoEncodingPossibleException(String msg) {
        super(msg);
    }
}
