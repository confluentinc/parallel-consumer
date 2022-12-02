package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.PCRetriableException;
import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.internal.Controller.addInstanceMDC;
import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PROTECTED;

/**
 * @author Antony Stubbs
 */
@Slf4j
@SuperBuilder
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class FunctionRunner<K, V, R> {

    /**
     * Key for the work container descriptor that will be added to the {@link MDC diagnostic context} while inside a
     * user function.
     */
    public static final String MDC_WORK_CONTAINER_DESCRIPTOR = "offset";

    @NonNull
    PCModule<K, V> module;

    @NonNull
    ParallelConsumerOptions<K, V> options;

    @NonNull
    Function<PollContextInternal<K, V>, List<R>> userFunctionWrapped;

    @NonNull
    Consumer<R> callback;

    @Getter(PROTECTED)
    @NonNull
    WorkMailbox<K, V> workMailbox;

    @NonNull
    WorkManager<K, V> workManager;

    protected void run(List<WorkContainer<K, V>> batch) {
        // for each record, construct dispatch to the executor and capture a Future
        log.trace("Sending work ({}) to pool", batch);

        addInstanceMDC(options);
        runUserFunction(batch);

//        // for a batch, each message in the batch shares the same result
//        for (WorkContainer<K, V> workContainer : batch) {
//            workContainer.setFuture(tuples);
//        }
    }

    /**
     * Run the supplied function.
     */
    // todo return value never used
    private List<ParallelConsumer.Tuple<ConsumerRecord<K, V>, R>> runUserFunction(List<WorkContainer<K, V>> workContainerBatch) {
        // call the user's function
        List<R> resultsFromUserFunction;
        PollContextInternal<K, V> context = new PollContextInternal<>(workContainerBatch);

        try {
            if (log.isDebugEnabled()) {
                // first offset of the batch
                MDC.put(MDC_WORK_CONTAINER_DESCRIPTOR, workContainerBatch.get(0).offset() + "");
            }
            log.trace("Pool received: {}", workContainerBatch);

            //
            boolean workIsStale = workManager.checkIfWorkIsStale(workContainerBatch);
            if (workIsStale) {
                // when epoch's change, we can't remove them from the executor pool queue, so we just have to skip them when we find them
                log.debug("Pool found work from old generation of assigned work, skipping message as epoch doesn't match current {}", workContainerBatch);
                return null;
            }

            resultsFromUserFunction = this.userFunctionWrapped.apply(context);

            for (final WorkContainer<K, V> kvWorkContainer : workContainerBatch) {
                onUserFunctionSuccess(kvWorkContainer, resultsFromUserFunction);
            }

            // capture each result, against the input record
            var intermediateResults = new ArrayList<ParallelConsumer.Tuple<ConsumerRecord<K, V>, R>>();
            for (R result : resultsFromUserFunction) {
                log.trace("Running users call back...");
                callback.accept(result);
            }

            // fail or succeed, either way we're done
            for (var kvWorkContainer : workContainerBatch) {
                addToMailBoxOnUserFunctionSuccess(context, kvWorkContainer, resultsFromUserFunction);
            }
            log.trace("User function future registered");

            return intermediateResults;
        } catch (Exception e) {
            // handle fail
            var cause = e.getCause();
            String msg = msg("Exception caught in user function running stage, registering WC as failed, returning to" +
                    " mailbox. Context: {}", context, e);
            if (cause instanceof PCRetriableException) {
                log.debug("Explicit " + PCRetriableException.class.getSimpleName() + " caught, logging at DEBUG only. " + msg, e);
            } else {
                log.error(msg, e);
            }

            for (var wc : workContainerBatch) {
                wc.onUserFunctionFailure(e);
                workMailbox.addToMailbox(context, wc); // always add on error
            }
            throw e; // trow again to make the future failed
        } finally {
            context.getProducingLock().ifPresent(ProducerManager.ProducingLock::unlock);
        }
    }

    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        log.trace("User function success");
        wc.onUserFunctionSuccess();
    }

    protected void addToMailBoxOnUserFunctionSuccess(PollContextInternal<K, V> context, WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        workMailbox.addToMailbox(context, wc);
    }

}

