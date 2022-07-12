package io.confluent.parallelconsumer.offsets;

import lombok.experimental.StandardException;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@StandardException

public class RunlengthV1EncodingNotSupported extends EncodingNotSupportedException {
    public RunlengthV1EncodingNotSupported(final String msg) {
        super(msg);
    }
}
