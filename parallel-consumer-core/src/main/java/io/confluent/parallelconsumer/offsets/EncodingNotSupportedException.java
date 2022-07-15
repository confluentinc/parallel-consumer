package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalException;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
public class EncodingNotSupportedException extends InternalException {
    public EncodingNotSupportedException(final String message) {
        super(message);
    }
}
