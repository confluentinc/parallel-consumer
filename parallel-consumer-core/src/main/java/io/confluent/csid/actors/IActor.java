package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * todo docs
 *
 * @param <T>
 * @author Antony Stubbs
 */
public interface IActor<T> {

    void tell(Consumer<T> action);

    <R> Future<R> ask(Function<T, R> action);

    boolean isEmpty();
}
