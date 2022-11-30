package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.internal.ExternalEngine;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Adapter for using Project Reactor as the asynchronous execution engine
 */
@Slf4j
public class ReactorProcessor<K, V> extends ExternalEngine<K, V> {

    private ReactorRunner<K, V, Object> runner;
    private final Supplier<Scheduler> scheduleSupplier;

    public ReactorProcessor(ParallelConsumerOptions<K, V> options, Supplier<Scheduler> newSchedulerSupplier) {
        super(options);
        this.scheduleSupplier = newSchedulerSupplier;
    }

    public ReactorProcessor(ParallelConsumerOptions<K, V> options) {
        this(options, null);
    }

    @SneakyThrows
    @Override
    public void close(Duration timeout, DrainingMode drainMode) {
        super.close(timeout, drainMode);
    }

    /**
     * Register a function to be to polled messages.
     * <p>
     * Make sure that you do any work immediately in a Publisher / Flux - do not block this thread.
     * <p>
     *
     * @param reactorFunction user function that takes a single record, and returns some type of Publisher to process
     *                        their work.
     * @see #react(Function)
     * @see ParallelConsumerOptions
     * @see ParallelConsumerOptions#batchSize
     * @see io.confluent.parallelconsumer.ParallelStreamProcessor#poll
     */
    public void react(Function<PollContext<K, V>, Publisher<?>> reactorFunction) {
        var wrappedUserFunc = runner.react(reactorFunction);

        this.runner = ReactorRunner.<K, V, Object>builder()
                .userFunctionWrapped(wrappedUserFunc)
                .options(options)
                .workMailbox(getWorkMailbox())
                .workManager(getWm())
                .schedulerSupplier(scheduleSupplier)
                .build();

        //
        Consumer<Object> voidCallBack = ignore -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);
    }

}
