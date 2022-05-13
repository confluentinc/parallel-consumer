package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.*;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.TerminalFailureReaction.SHUTDOWN;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.TerminalFailureReaction.SKIP;
import static org.slf4j.event.Level.DEBUG;
import static org.slf4j.event.Level.WARN;

@Value

@Slf4j
public class FunctionRunnerThing<K, V> {

    AbstractParallelEoSStreamProcessor pc;

    /**
     * Run the supplied function.
     */
    // todo extract class from this point
    protected <R> List<ParallelConsumer.Tuple<ConsumerRecord<K, V>, R>> runUserFunction(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                                                                        Consumer<R> callback,
                                                                                        List<WorkContainer<K, V>> workContainerBatch) {
        // catch and process any internal error
        try {
            if (log.isDebugEnabled()) {
                // first offset of the batch
                MDC.put("offset", workContainerBatch.get(0).offset() + "");
            }
            log.trace("Pool received: {}", workContainerBatch);

            //
            boolean workIsStale = pc.getWm().checkIfWorkIsStale(workContainerBatch);
            if (workIsStale) {
                // when epoch's change, we can't remove them from the executor pool queue, so we just have to skip them when we find them
                log.debug("Pool found work from old generation of assigned work, skipping message as epoch doesn't match current {}", workContainerBatch);
                return null;
            }

            PollContextInternal<K, V> context = new PollContextInternal<>(workContainerBatch);

            List<R> resultsFromUserFunction = runUserFunction(usersFunction, context);

            return handleUserSuccess(callback, workContainerBatch, resultsFromUserFunction);
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

    private <R> ArrayList<ParallelConsumer.Tuple<ConsumerRecord<K, V>, R>> handleUserSuccess(Consumer<R> callback, List<WorkContainer<K, V>> workContainerBatch, List<R> resultsFromUserFunction) {
        for (final WorkContainer<K, V> kvWorkContainer : workContainerBatch) {
            pc.onUserFunctionSuccess(kvWorkContainer, resultsFromUserFunction);
        }

        // capture each result, against the input record
        var intermediateResults = new ArrayList<ParallelConsumer.Tuple<ConsumerRecord<K, V>, R>>();
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

    private <R> List<R> runUserFunction(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                        PollContextInternal<K, V> context) {
        try {
            return usersFunction.apply(context);
        } catch (PCTerminalException e) {
            var reaction = pc.getOptions().getTerminalFailureReaction();

            if (reaction == SKIP) {
                log.warn("Terminal error in user function, skipping record due to configuration in {} - triggering context: {}",
                        ParallelConsumerOptions.class.getSimpleName(),
                        context);

                // return empty result to cause system to skip as if it succeeded
                // todo call on success to do all the extra stuff
                return new ArrayList<>();
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
        } catch (Exception e) {
            handleUserRetriableFailure(context, e);

            // throw again to make the future failed
            throw e;
        }
    }

    private void handleUserRetriableFailure(PollContextInternal<K, V> context, Exception e) {
        logUserFunctionException(e);
        markRecordsFailed(context.getWorkContainers(), e);
    }

    private void markRecordsFailed(List<WorkContainer<K, V>> workContainerBatch, Exception e) {
        for (var wc : workContainerBatch) {
            wc.onUserFunctionFailure(e);
            pc.addToMailbox(wc); // always add on error
        }
    }

    /**
     * If user explicitly throws the {@link PCRetriableException}, then don't log it, as the user is already aware.
     * <p>
     * <a href=https://english.stackexchange.com/questions/305273/retriable-or-retryable#305274>Retriable or
     * Retryable?</a> Kafka uses Retriable, so we'll go with that ;)
     */
    private void logUserFunctionException(Exception e) {
        boolean explicitlyRetryable = e instanceof PCRetriableException;
        var level = explicitlyRetryable ? DEBUG : WARN;
        var prefix = explicitlyRetryable ? "Explicit " + PCRetriableException.class.getSimpleName() + " caught - " : "";
        var message = prefix + "Exception in user function, registering record as failed, returning to queue";
        log.atLevel(level).log(message, e);
    }
}

