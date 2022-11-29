package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.PCRetriableException;
import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
@Value
public class FunctionRunner<K, V, R> {

    PCModule<K, V> module;
    ParallelConsumerOptions<K, V> options;

    Function<PollContextInternal<K, V>, List<R>> userFunctionWrapped;

    Consumer<R> callback;

    public void run(Consumer<Void> callback, WorkContainer<K, V> work) {

    }


    /**
     * Submit a piece of work to the processing pool.
     *
     * @param workToProcess the polled records to process
     */
    protected <R> void submitWorkToPool(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                        Consumer<R> callback,
                                        List<WorkContainer<K, V>> workToProcess) {
        if (!workToProcess.isEmpty()) {
            log.debug("New work incoming: {}, Pool stats: {}", workToProcess.size(), workerThreadPool);

            // perf: could inline makeBatches
            var batches = makeBatches(workToProcess);

            // debugging
            if (log.isDebugEnabled()) {
                var sizes = batches.stream().map(List::size).sorted().collect(Collectors.toList());
                log.debug("Number batches: {}, smallest {}, sizes {}", batches.size(), sizes.stream().findFirst().get(), sizes);
                List<Integer> integerStream = sizes.stream().filter(x -> x < (int) options.getBatchSize()).collect(Collectors.toList());
                if (integerStream.size() > 1) {
                    log.warn("More than one batch isn't target size: {}. Input number of batches: {}", integerStream, batches.size());
                }
            }

            // submit
            for (var batch : batches) {
                submitWorkToPoolInner(usersFunction, callback, batch);
            }
        }
    }

    private <R> void submitWorkToPoolInner(final Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                           final Consumer<R> callback,
                                           final List<WorkContainer<K, V>> batch) {
        // for each record, construct dispatch to the executor and capture a Future
        log.trace("Sending work ({}) to pool", batch);
        Future outputRecordFuture = workerThreadPool.submit(() -> {
            addInstanceMDC();
            return runUserFunction(usersFunction, callback, batch);
        });
        // for a batch, each message in the batch shares the same result
        for (final WorkContainer<K, V> workContainer : batch) {
            workContainer.setFuture(outputRecordFuture);
        }
    }

    private List<List<WorkContainer<K, V>>> makeBatches(List<WorkContainer<K, V>> workToProcess) {
        int maxBatchSize = options.getBatchSize();
        return partition(workToProcess, maxBatchSize);
    }

    private static <T> List<List<T>> partition(Collection<T> sourceCollection, int maxBatchSize) {
        List<List<T>> listOfBatches = new ArrayList<>();
        List<T> batchInConstruction = new ArrayList<>();

        //
        for (T item : sourceCollection) {
            batchInConstruction.add(item);

            //
            if (batchInConstruction.size() == maxBatchSize) {
                listOfBatches.add(batchInConstruction);
                batchInConstruction = new ArrayList<>();
            }
        }

        // add partial tail
        if (!batchInConstruction.isEmpty()) {
            listOfBatches.add(batchInConstruction);
        }

        log.debug("sourceCollection.size() {}, batches: {}, batch sizes {}",
                sourceCollection.size(),
                listOfBatches.size(),
                listOfBatches.stream().map(List::size).collect(Collectors.toList()));
        return listOfBatches;
    }


    /**
     * Run the supplied function.
     */
    protected <R> List<ParallelConsumer.Tuple<ConsumerRecord<K, V>, R>> runUserFunction(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                                                                        Consumer<R> callback,
                                                                                        List<WorkContainer<K, V>> workContainerBatch) {
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
            boolean workIsStale = wm.checkIfWorkIsStale(workContainerBatch);
            if (workIsStale) {
                // when epoch's change, we can't remove them from the executor pool queue, so we just have to skip them when we find them
                log.debug("Pool found work from old generation of assigned work, skipping message as epoch doesn't match current {}", workContainerBatch);
                return null;
            }

            resultsFromUserFunction = usersFunction.apply(context);

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
                addToMailbox(context, wc); // always add on error
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
        addToMailbox(context, wc);
    }

}
