package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.ParallelConsumerInternalException;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
public class EncodingNotSupportedException extends ParallelConsumerInternalException {
    public EncodingNotSupportedException(final String message) {
        super(message);
    }
}
