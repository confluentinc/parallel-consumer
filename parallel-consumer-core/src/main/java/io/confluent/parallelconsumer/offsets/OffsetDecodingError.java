package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
public class OffsetDecodingError extends Exception {
    public OffsetDecodingError(final String s, final IllegalArgumentException a) {
        super(s, a);
    }
}
