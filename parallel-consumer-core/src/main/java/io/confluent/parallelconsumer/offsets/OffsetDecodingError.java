package io.confluent.parallelconsumer.offsets;

import io.confluent.parallelconsumer.internal.InternalException;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
public class OffsetDecodingError extends InternalException {
    public OffsetDecodingError(final String s, final IllegalArgumentException a) {
        super(s, a);
    }
}
