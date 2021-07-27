package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.ExternalEngine;
import io.confluent.parallelconsumer.state.ShardManager;
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

import static io.confluent.csid.utils.StringUtils.msg;
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

    public ReactorProcessor(ParallelConsumerOptions options, Supplier<Scheduler> newSchedulerSupplier) {
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

    /**
     * Make sure that you do any work immediately in a Publisher / Flux - do not block this thread.
     *
     * @param reactorFunction user function that takes a single record, and returns some type of Publisher to process
     *                        their work.
     */
    public void react(Function<ConsumerRecord<K, V>, Publisher<?>> reactorFunction) {
        Function<ConsumerRecord<K, V>, List<Object>> wrappedUserFunc = (rec) -> {
            log.trace("asyncPoll - Consumed a record ({}), executing void function...", rec.offset());

            // attach internal handler
            ShardManager<K, V> shard = wm.getSm();
            WorkContainer<K, V> wc = shard.getWorkContainerForRecord(rec);
            if (wc == null) {
                // throw it - will retry
                // should be fixed by moving for TreeMap to ConcurrentSkipListMap
                throw new IllegalStateException(msg("WC for record is null! {}", rec));
            } else {
                wc.setWorkType(REACTOR_TYPE);
            }

            Publisher<?> publisher = carefullyRun(reactorFunction, rec);
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
                        wc.onUserFunctionSuccess();
                        addToMailbox(wc);
                    })
                    .doOnError(throwable -> {
                        log.error("Reactor fail signal", throwable);
                        wc.onUserFunctionFailure();
                        addToMailbox(wc);
                    })
                    // cause users Publisher to run a thread pool, if it hasn't already - this is a crucial magical part
                    .subscribeOn(getScheduler())
                    .subscribe();

            log.trace("asyncPoll - user function finished ok.");
            return UniLists.of(flux);
        };

        Consumer<Object> voidCallBack = (ignore) -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);

    }

    private Scheduler getScheduler() {
        return this.schedulerSupplier.get();
    }

}
