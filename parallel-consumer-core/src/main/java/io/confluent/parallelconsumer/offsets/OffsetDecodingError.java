package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalException;

/*-
 * Error decoding offsets
 *
 * TODO should extend java.lang.Error ?
 */
public class OffsetDecodingError extends InternalException {
    public OffsetDecodingError(final String s, final IllegalArgumentException a) {
        super(s, a);
    }
}
