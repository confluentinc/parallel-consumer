package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalException;
import lombok.experimental.StandardException;

/**
 * Parent of the exceptions for when the {@link OffsetEncoder} cannot encode the given data.
 *
 * @author Antony Stubbs
 */
@StandardException
public class EncodingNotSupportedException extends InternalException {
}
