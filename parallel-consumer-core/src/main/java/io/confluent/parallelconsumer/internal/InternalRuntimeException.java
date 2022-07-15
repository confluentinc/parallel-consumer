package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.utils.StringUtils;
import lombok.experimental.StandardException;

/**
 * Internal {@link RuntimeException}
 *
 * @author Antony Stubbs
 * @see InternalException
 */
@StandardException
public class InternalRuntimeException extends RuntimeException {

    /**
     * @see StringUtils#msg(String, Object...)
     */
    public static InternalRuntimeException msg(String message, Object... vars) {
        return new InternalRuntimeException(StringUtils.msg(message, vars));
    }

    /**
     * @see StringUtils#msg(String, Object...)
     */
    public InternalRuntimeException(String message, Throwable e, Object... args) {
        this(StringUtils.msg(message, args), e);
    }

}
