package io.confluent.parallelconsumer.internal;

import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.lang.Nullable;
import lombok.Data;
import lombok.Setter;

import java.util.ArrayDeque;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Data
public class PCWorkerPool<K, V, R> {

    @Setter
    @Nullable
    FunctionRunner<K, V, R> functionRunner = null;

    final List<PCWorker<K, V>> workers;

    public PCWorkerPool(int poolSize) {
        workers = Range.range(poolSize).toStream().boxed()
                .map(ignore -> new PCWorker<K, V>(this.functionRunner))
                .collect(Collectors.toList());
    }

    public int getCapacity(Timer workRetrievalTimer) {
        return workers.stream().map(worker -> worker.getQueueCapacity(workRetrievalTimer)).reduce(Integer::sum).orElse(0);
    }

    /**
     * Distribute the work in this list fairly across the workers
     */
    public void distribute(List<? extends WorkContainer<K, V>> work) {
        var queue = new ArrayDeque<>(work);
        for (PCWorker<K, V> worker : workers) {
            var poll = ofNullable(queue.poll());
            poll.ifPresent(worker::enqueue);
        }
    }
}
