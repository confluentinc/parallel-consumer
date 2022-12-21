package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

/**
 * A copy of Java's {@link java.util.function.Function}, but which can throw exceptions.
 *
 * @author Antony Stubbs
 * @see java.util.function.Function
 * @see Actor#ask(FunctionWithException)
 */
@FunctionalInterface
public interface FunctionWithException<T, R> {

    /**
     * @see java.util.function.Function#apply
     */
    R apply(T t) throws Exception;

}
