package io.confluent.parallelconsumer.internal;

import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.micrometer.core.instrument.Timer;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Slf4j
@Value
@NonFinal
public class PCWorkerPool<K, V, R> {

    ParallelConsumerOptions<K, V> pcOptions;

    FunctionRunner<K, V, R> runner;

    List<PCWorker<K, V, R>> workers;

    public PCWorkerPool(int poolSize, FunctionRunner<K, V, R> functionRunner, ParallelConsumerOptions<K, V> options) {
        this.runner = functionRunner;
        this.pcOptions = options;
        workers = Range.range(poolSize).toStream().boxed()
                .map(ignore -> new PCWorker<>(this))
                .collect(Collectors.toList());
    }

    public int getCapacity(Timer workRetrievalTimer) {
        return workers.stream().map(worker -> worker.getQueueCapacity(workRetrievalTimer)).reduce(Integer::sum).orElse(0);
    }

    /**
     * Distribute the work in this list fairly across the workers
     */
    public void distribute(List<WorkContainer<K, V>> workToProcess) {
        var batches = makeBatches(workToProcess);

        var queue = new ArrayDeque<>(batches);
        for (var worker : workers) {
            var poll = ofNullable(queue.poll());
            poll.ifPresent(worker::enqueue); // todo object allocation warning
        }
    }

    private List<List<WorkContainer<K, V>>> makeBatches(List<WorkContainer<K, V>> workToProcess) {
        int maxBatchSize = pcOptions.getBatchSize();
        var batches = partition(workToProcess, maxBatchSize);

        // debugging
        if (log.isDebugEnabled()) {
            var sizes = batches.stream().map(List::size).sorted().collect(Collectors.toList());
            log.debug("Number batches: {}, smallest {}, sizes {}", batches.size(), sizes.stream().findFirst().get(), sizes);
            List<Integer> integerStream = sizes.stream().filter(x -> x < (int) pcOptions.getBatchSize()).collect(Collectors.toList());
            if (integerStream.size() > 1) {
                log.warn("More than one batch isn't target size: {}. Input number of batches: {}", integerStream, batches.size());
            }
        }

        return batches;
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
                batchInConstruction = new ArrayList<>(); // todo object allocation warning
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

}
