package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.internal.ExternalEngine;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;
import pl.tlinkowski.unij.api.UniLists;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.internal.UserFunctions.carefullyRun;

/**
 * Adapter for using Project Reactor as the asynchronous execution engine
 */
@Slf4j
public class ReactorProcessor<K, V> extends ExternalEngine<K, V> {

    /**
     * @see WorkContainer#getWorkType()
     */
    private static final String REACTOR_TYPE = "reactor.x-type";

    private final Supplier<Scheduler> schedulerSupplier;
    private final Supplier<Scheduler> defaultSchedulerSupplier = Schedulers::boundedElastic;

    public ReactorProcessor(ParallelConsumerOptions<K, V> options, Supplier<Scheduler> newSchedulerSupplier) {
        super(options);
        this.schedulerSupplier = (newSchedulerSupplier == null) ? defaultSchedulerSupplier : newSchedulerSupplier;
    }

    public ReactorProcessor(ParallelConsumerOptions<K, V> options) {
        this(options, null);
    }

    @Override
    protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
        for (Object object : resultsFromUserFunction) {
            return (object instanceof Disposable);
        }
        return false;
    }

    @SneakyThrows
    @Override
    public void close(Duration timeout, DrainingMode drainMode) {
        super.close(timeout, drainMode);
    }

//    /**
//     * Like {@link ParallelStreamProcessor#pollBatch} but for Reactor.
//     * <p>
//     * Register a function to be applied to a batch of messages.
//     * <p>
//     * Make sure that you do any work immediately in a Publisher / Flux - do not block this thread.
//     * <p>
//     * The system will treat the messages as a set, so if an error is thrown by the user code, then all messages will be
//     * marked as failed and be retried (Note that when they are retried, there is no guarantee they will all be in the
//     * same batch again). So if you're going to process messages individually, then don't use this function.
//     * <p>
//     * Otherwise, if you're going to process messages in sub sets from this batch, it's better to instead adjust the
//     * {@link ParallelConsumerOptions#getBatchSize()} instead to the actual desired size, and process them as a whole.
//     *
//     * @param reactorFunction user function that takes a single record, and returns some type of Publisher to process
//     *                        their work.
//     * @see ParallelStreamProcessor#pollBatch
//     * @see ParallelConsumerOptions#getBatchSize()
//     */

    /**
     * Make sure that you do any work immediately in a Publisher / Flux - do not block this thread.
     * <p>
     * Like {@link #react} but no batching - single message at a time.
     *
     * @param reactorFunction user function that takes a single record, and returns some type of Publisher to process
     *                        their work.
     * @see #react(Function)
     */
    public void react(Function<PollContext<K, V>, Publisher<?>> reactorFunction) {
        Function<PollContext<K, V>, List<Object>> wrappedUserFunc = (pollContext) -> {

            if (log.isTraceEnabled()) {
                log.trace("Record list ({}), executing void function...",
                        pollContext.streamConsumerRecords()
                                .map(ConsumerRecord::offset)
                                .collect(Collectors.toList())
                );
            }

            // attach internal handler
            pollContext.getWorkContainers()
                    .forEach(x -> x.setWorkType(REACTOR_TYPE));

            Publisher<?> publisher = carefullyRun(reactorFunction, pollContext);

            Disposable flux = Flux.from(publisher)
                    // using #subscribeOn so this should be redundant, but testing has shown otherwise
                    // note this will not cause user's function to run in pool - without successful use of subscribeOn,
                    // it will run in the controller thread, unless user themselves uses either publishOn or successful
                    // subscribeOn
                    .publishOn(getScheduler())
                    .doOnNext(signal -> {
                        log.trace("doOnNext {}", signal);
                    })
                    .doOnComplete(() -> {
                        log.debug("Reactor success (doOnComplete)");
                        for (var wc : pollContext.getWorkContainers()) {
                            wc.onUserFunctionSuccess();
                            addToMailbox(wc);
                        }
                    })
                    .doOnError(throwable -> {
                        log.error("Reactor fail signal", throwable);
                        for (var wc : pollContext.getWorkContainers()) {
                            wc.onUserFunctionFailure();
                            addToMailbox(wc);
                        }
                    })
                    // cause users Publisher to run a thread pool, if it hasn't already - this is a crucial magical part
                    .subscribeOn(getScheduler())
                    .subscribe();

            log.trace("asyncPoll - user function finished ok.");
            return UniLists.of(flux);
        };

        //
        Consumer<Object> voidCallBack = (ignore) -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);
    }

//    /**
//     * Make sure that you do any work immediately in a Publisher / Flux - do not block this thread.
//     * <p>
//     * Like {@link #reactBatch} but no batching - single message at a time.
//     *
//     * @param reactorFunction user function that takes a single record, and returns some type of Publisher to process
//     *                        their work.
//     * @see #reactBatch(Function)
//     */
//    public void react(Function<PollContext<K, V>, Publisher<?>> reactorFunction) {
//        // wrap single record function in batch function
//        Function<PollContext<K, V>, Publisher<?>> batchReactorFunctionWrapper = (context) -> {
//            log.trace("Consumed a record ({}), executing void function...", context);
//
//            Publisher<?> publisher = carefullyRun(reactorFunction, context);
//
//            log.trace("asyncPoll - user function finished ok.");
//            return publisher;
//        };
//
//        //
//        reactBatch(batchReactorFunctionWrapper);
//    }

    private Scheduler getScheduler() {
        return this.schedulerSupplier.get();
    }

}
