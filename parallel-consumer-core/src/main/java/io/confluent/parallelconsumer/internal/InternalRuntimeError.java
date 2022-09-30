package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.StringUtils;
import lombok.experimental.StandardException;

/**
 * Generic internal runtime error
 */
@StandardException
public class InternalRuntimeError extends RuntimeException {
    public InternalRuntimeError(String message, Throwable e, Object... args) {
        this(StringUtils.msg(message, args), e);
    }
}
