package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.*;
import io.confluent.parallelconsumer.ParallelConsumer.Tuple;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.TerminalFailureReaction.SHUTDOWN;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.TerminalFailureReaction.SKIP;
import static io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.MDC_WORK_CONTAINER_DESCRIPTOR;

@AllArgsConstructor
@Slf4j
public class UserFunctionRunner<K, V> {

    private AbstractParallelEoSStreamProcessor<K, V> pc;

    /**
     * Run the supplied function.
     */
    protected <R> List<Tuple<ConsumerRecord<K, V>, R>> runUserFunction(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                                                       Consumer<R> callback,
                                                                       List<WorkContainer<K, V>> workContainerBatch) {
        // catch and process any internal error
        try {
            if (log.isDebugEnabled()) {
                // first offset of the batch
                MDC.put(MDC_WORK_CONTAINER_DESCRIPTOR, workContainerBatch.get(0).offset() + "");
            }
            log.trace("Pool received: {}", workContainerBatch);

            //
            boolean workIsStale = pc.getWm().checkIfWorkIsStale(workContainerBatch);
            if (workIsStale) {
                // when epoch's change, we can't remove them from the executor pool queue, so we just have to skip them when we find them
                log.debug("Pool found work from old generation of assigned work, skipping message as epoch doesn't match current {}", workContainerBatch);
                return UniLists.of();
            }

            PollContextInternal<K, V> context = new PollContextInternal<>(workContainerBatch);

            return runWithUserExceptions(usersFunction, context, callback);
        } catch (PCUserException e) {
            // throw again to make the future failed
            throw e;
        } catch (Exception e) {
            log.error("Unknown internal error handling user function dispatch, terminating");

            pc.closeDontDrainFirst();

            // throw again to make the future failed
            throw e;
        }
    }

    private <R> List<Tuple<ConsumerRecord<K, V>, R>> runWithUserExceptions(
            Function<? super PollContextInternal<K, V>, ? extends List<R>> usersFunction,
            PollContextInternal<K, V> context,
            Consumer<R> callback) {
        try {
            var resultsFromUserFunction = usersFunction.apply(context);
            return handleUserSuccess(callback, context.getWorkContainers(), resultsFromUserFunction);
        } catch (PCTerminalException e) {
            return handleUserTerminalFailure(context, e, callback);
        } catch (PCRetriableException e) {
            handleExplicitUserRetriableFailure(context, e);

            // throw again to make the future failed
            throw e;
        } catch (Exception e) {
            handleImplicitUserRetriableFailure(context, e);

            // throw again to make the future failed
            throw e;
        }
    }

    private <R> List<Tuple<ConsumerRecord<K, V>, R>> handleUserSuccess(Consumer<R> callback,
                                                                       List<WorkContainer<K, V>> workContainerBatch,
                                                                       List<R> resultsFromUserFunction) {
        for (final WorkContainer<K, V> kvWorkContainer : workContainerBatch) {
            pc.onUserFunctionSuccess(kvWorkContainer, resultsFromUserFunction);
        }

        // capture each result, against the input record
        var intermediateResults = new ArrayList<Tuple<ConsumerRecord<K, V>, R>>();
        for (R result : resultsFromUserFunction) {
            log.trace("Running users call back...");
            callback.accept(result);
        }

        // fail or succeed, either way we're done
        for (var kvWorkContainer : workContainerBatch) {
            pc.addToMailBoxOnUserFunctionSuccess(kvWorkContainer, resultsFromUserFunction);
        }

        log.trace("User function future registered");
        return intermediateResults;
    }

    private <R> List<Tuple<ConsumerRecord<K, V>, R>> handleUserTerminalFailure(PollContextInternal<K, V> context,
                                                                               PCTerminalException e, Consumer<R> callback) {
        var reaction = pc.getOptions().getTerminalFailureReaction();

        if (reaction == SKIP) {
            log.warn("Terminal error in user function, skipping record due to configuration in {} - triggering context: {}",
                    ParallelConsumerOptions.class.getSimpleName(),
                    context);

            // return empty result to cause system to skip as if it succeeded
            return handleUserSuccess(callback, context.getWorkContainers(), UniLists.of());
        } else if (reaction == SHUTDOWN) {
            log.error("Shutting down upon terminal failure in user function due to {} {} setting in {} - triggering context: {}",
                    reaction,
                    ParallelConsumerOptions.TerminalFailureReaction.class.getSimpleName(),
                    ParallelConsumerOptions.class.getSimpleName(),
                    context);

            pc.closeDontDrainFirst();

            // throw again to make the future failed
            throw e;
        } else {
            throw new InternalRuntimeError(msg("Unsupported reaction config ({}) - submit a bug report.", reaction));
        }
    }

    private void handleExplicitUserRetriableFailure(PollContextInternal<K, V> context, PCRetriableException e) {
        logUserFunctionException(e);

        Optional<Offsets> offsetsOptional = e.getOffsetsOptional();
        if (offsetsOptional.isPresent()) {
            Offsets offsets = offsetsOptional.get();
            log.debug("Specific offsets present in {}", offsets);
            context.streamInternal()
                    .forEach(work -> {
                        if (offsets.contains(work)) {
                            markRecordFailed(e, work.getWorkContainer());
                        } else {
                            // mark record succeeded
                            handleUserSuccess();
                        }
                    });
        } else {
            markRecordsFailed(context.getWorkContainers(), e);
        }
    }

    private void handleImplicitUserRetriableFailure(PollContextInternal<K, V> context, Exception e) {
        logUserFunctionException(e);
        markRecordsFailed(context.getWorkContainers(), e);
    }

    private void markRecordsFailed(List<WorkContainer<K, V>> workContainerBatch, Exception e) {
        for (var wc : workContainerBatch) {
            markRecordFailed(e, wc);
        }
    }

    private void markRecordFailed(Exception e, WorkContainer<K, V> wc) {
        wc.onUserFunctionFailure(e);
        pc.addToMailbox(wc); // always add on error
    }

    /**
     * If user explicitly throws the {@link PCRetriableException}, then don't log it, as the user is already aware.
     * <p>
     * <a href=https://english.stackexchange.com/questions/305273/retriable-or-retryable#305274>Retriable or
     * Retryable?</a> Kafka uses Retriable, so we'll go with that ;)
     */
    private void logUserFunctionException(Exception e) {
        var message = "in user function, registering record as failed, returning to queue";
        if (e instanceof PCRetriableException) {
            log.debug("Explicit exception {} caught - {}", message, e.toString());
        } else {
            log.warn("Exception {}", message, e);
        }
    }

}

