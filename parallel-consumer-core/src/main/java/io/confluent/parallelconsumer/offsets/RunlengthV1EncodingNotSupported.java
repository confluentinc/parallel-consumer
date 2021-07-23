package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
public class RunlengthV1EncodingNotSupported extends EncodingNotSupportedException {
    public RunlengthV1EncodingNotSupported(final String msg) {
        super(msg);
    }
}
