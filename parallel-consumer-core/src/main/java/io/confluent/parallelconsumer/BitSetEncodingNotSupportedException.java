package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
public class BitSetEncodingNotSupportedException extends EncodingNotSupportedException {

    public BitSetEncodingNotSupportedException(String msg) {
        super(msg);
    }

}
