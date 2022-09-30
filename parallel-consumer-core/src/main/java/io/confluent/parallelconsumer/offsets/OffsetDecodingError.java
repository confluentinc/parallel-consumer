package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalException;
import lombok.experimental.StandardException;

/*-
 * Error decoding offsets
 *
 * TODO should extend java.lang.Error ?
 *
 * @author Antony Stubbs
 */
@StandardException
public class OffsetDecodingError extends InternalException {
}
