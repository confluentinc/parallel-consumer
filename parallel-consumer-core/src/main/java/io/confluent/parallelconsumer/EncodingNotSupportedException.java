package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
public class EncodingNotSupportedException extends Exception {
    public EncodingNotSupportedException(final String message) {
        super(message);
    }
}
