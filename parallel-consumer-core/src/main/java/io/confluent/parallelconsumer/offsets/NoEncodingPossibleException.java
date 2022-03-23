package io.confluent.parallelconsumer.offsets;

import io.confluent.parallelconsumer.internal.ParallelConsumerInternalException;

public class NoEncodingPossibleException extends ParallelConsumerInternalException {

    public NoEncodingPossibleException(String msg) {
        super(msg);
    }
}
