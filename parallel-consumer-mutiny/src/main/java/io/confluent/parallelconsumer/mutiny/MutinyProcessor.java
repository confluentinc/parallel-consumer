package io.confluent.parallelconsumer.mutiny;

/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 */

import io.confluent.parallelconsumer.PCRetriableException;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.internal.ExternalEngine;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.Cancellable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.internal.UserFunctions.carefullyRun;

/**
 * Adapter for using Mutiny as the asynchronous execution engine.
 */
@Slf4j
public class MutinyProcessor<K, V> extends ExternalEngine<K, V> {

    /**
     * @see WorkContainer#getWorkType()
     */
    private static final String MUTINY_TYPE = "mutiny.x-type";

    private final Supplier<Executor> executorSupplier;
    private final Supplier<Executor> defaultExecutorSupplier = Infrastructure::getDefaultWorkerPool;

    public MutinyProcessor(ParallelConsumerOptions<K, V> options, Supplier<Executor> newExecutorSupplier) {
        super(options);
        this.executorSupplier = (newExecutorSupplier == null) ? defaultExecutorSupplier : newExecutorSupplier;
    }
    
    public MutinyProcessor(ParallelConsumerOptions<K, V> options) {
        this(options, null);
    }

    @Override
    protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
        for (Object object : resultsFromUserFunction) {
            return (object instanceof io.smallrye.mutiny.subscription.Cancellable);
        }
        return false;
    }

    @Override
    public void close(Duration timeout, DrainingMode drainMode) {
        super.close(timeout, drainMode);
    }

    /**
     * Register a function to be applied to polled messages.
     * <p>
     * Make sure that you do any work immediately - do not block this thread.
     * <p>
     *
     * @param mutinyFunction user function that takes a PollContext and returns a Uni<T>
     * @see #onRecord(Function)
     * @see ParallelConsumerOptions
     * @see ParallelConsumerOptions#batchSize
     * @see io.confluent.parallelconsumer.ParallelStreamProcessor#poll
     */

    /**
     * Register a function to be applied to polled messages.
     * This must return a Uni<Void> to signal async completion.
     *
     * @param mutinyFunction user function that takes a PollContext and returns a Uni<Void>
     */
    public <T> void onRecord(Function<PollContext<K, V>, Uni<T>> mutinyFunction) {

        Function<PollContextInternal<K, V>, List<Object>> wrappedUserFunc = pollContext -> {

            if (log.isTraceEnabled()) {
                log.trace("Record list ({}), executing void function...",
                        pollContext.streamConsumerRecords()
                                .map(ConsumerRecord::offset)
                                .collect(Collectors.toList())
                );
            }

            pollContext.streamWorkContainers()
                    .forEach(x -> x.setWorkType(MUTINY_TYPE));

            Cancellable uni = Uni.createFrom().deferred(() ->
                            carefullyRun(mutinyFunction, pollContext.getPollContext())
                    )
                    .onItem()
                    .transformToMulti(result -> {
                        if(result == null) {
                            return Multi.createFrom().empty();
                        }
                        else if (result instanceof Multi<?> multi) {
                            return multi;                 // unwrap Multi
                        } else {
                            return Multi.createFrom().item(result); // wrap single item as Multi
                        }
                    })
                    .onItem()
                    .invoke(signal -> log.trace("onItem {}", signal))
                    .runSubscriptionOn(getExecutor())
                    .subscribe().with(
                            ignored -> {},
                            throwable -> onError(pollContext, throwable),
                            () -> onComplete(pollContext)
                    );

            log.trace("asyncPoll - user function finished ok.");
            return UniLists.of(uni);
        };

        //
        Consumer<Object> voidCallBack = ignored -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);
    }

    private void onComplete(PollContextInternal<K, V> pollContext) {
        log.debug("Mutiny success");
        pollContext.streamWorkContainers().forEach(wc -> {
            wc.onUserFunctionSuccess();
            addToMailbox(pollContext, wc);
        });
    }

    private void onError(PollContextInternal<K, V> pollContext, Throwable throwable) {
        if (throwable instanceof PCRetriableException) {
            log.debug("Mutiny fail signal", throwable);
        } else {
            log.error("Mutiny fail signal", throwable);
        }
        pollContext.streamWorkContainers().forEach(wc -> {
            wc.onUserFunctionFailure(throwable);
            addToMailbox(pollContext, wc);
        });
    }

    private Executor getExecutor() {
        return this.executorSupplier.get();
    }
}
