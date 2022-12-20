package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Move docs here
 * <p>
 * todo docs
 *
 * @param <T>
 * @author Antony Stubbs
 * @see Actor
 */
// todo remove?
// todo rename
public interface IActor<T> {

    /**
     * Exceptions in execution will be logged
     *
     * @param action
     */
    void tell(Consumer<T> action);

    /**
     * Same as {@link IActor#tell} but messages will be placed at the front of the queue, instead of at the end.
     */
    void tellImmediately(Consumer<T> action);

    // todo use CompletableFuture instead of Future
    <R> Future<R> askImmediately(FunctionWithException<T, R> action);

    <R> Future<R> ask(FunctionWithException<T, R> action);

    boolean isEmpty();

    void processBlocking(Duration timeout) throws InterruptedException;

    // todo in interface?
    void process();

    String getActorName();

    void close();

    void start();

}
