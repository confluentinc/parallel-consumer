package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
public class BitSetEncodingNotSupportedException extends EncodingNotSupportedException {

    public BitSetEncodingNotSupportedException(String msg) {
        super(msg);
    }

}
