package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.internal.ExternalEngineRunner;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;
import pl.tlinkowski.unij.api.UniLists;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.internal.UserFunctions.carefullyRun;

/**
 * @author Antony Stubbs
 */
@Slf4j
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class ReactorRunner<K, V, R> extends ExternalEngineRunner<K, V, R> {

    /**
     * @see WorkContainer#getWorkType()
     */
    private static final String REACTOR_TYPE = "reactor.x-type";

    private static final Supplier<Scheduler> DEFAULT_SCHEDULE_SUPPLIER = Schedulers::boundedElastic;

    @Builder.Default
    private final Supplier<Scheduler> schedulerSupplier = DEFAULT_SCHEDULE_SUPPLIER;

    @Override
    protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
        for (Object object : resultsFromUserFunction) {
            return (object instanceof Disposable);
        }
        return false;
    }

    private Scheduler getScheduler() {
        return this.schedulerSupplier.get();
    }

    public Function<PollContextInternal<K, V>, List<Object>> react(Function<PollContext<K, V>, Publisher<?>> reactorFunction) {
        return pollContext -> reactInner(reactorFunction, pollContext);
    }

    private List<Object> reactInner(Function<PollContext<K, V>, Publisher<?>> reactorFunction, PollContextInternal<K, V> pollContext) {
        if (log.isTraceEnabled()) {
            log.trace("Record list ({}), executing void function...",
                    pollContext.streamConsumerRecords()
                            .map(ConsumerRecord::offset)
                            .collect(Collectors.toList())
            );
        }

        // attach internal handler
        pollContext.streamWorkContainers()
                .forEach(x -> x.setWorkType(REACTOR_TYPE));

        Publisher<?> publisher = carefullyRun(reactorFunction, pollContext.getPollContext());

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
                    pollContext.streamWorkContainers().forEach(wc -> {
                        wc.onUserFunctionSuccess();
                        getWorkMailbox().addToMailbox(pollContext, wc);
                    });
                })
                .doOnError(throwable -> {
                    log.error("Reactor fail signal", throwable);
                    pollContext.streamWorkContainers().forEach(wc -> {
                        wc.onUserFunctionFailure(throwable);
                        getWorkMailbox().addToMailbox(pollContext, wc);
                    });
                })
                // cause users Publisher to run a thread pool, if it hasn't already - this is a crucial magical part
                .subscribeOn(getScheduler())
                .subscribe();

        log.trace("asyncPoll - user function finished ok.");
        return UniLists.of(flux);
    }
}
