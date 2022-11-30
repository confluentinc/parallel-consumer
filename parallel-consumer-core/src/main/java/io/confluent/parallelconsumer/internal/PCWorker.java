package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.state.WorkContainer;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Antony Stubbs
 */
@Slf4j
@Value
public class PCWorker<K, V, R> {

    SimpleMeterRegistry metricsRegistry = new SimpleMeterRegistry();

    Timer userFunctionTimer = metricsRegistry.timer("user.function");

    WorkQueue<K, V> workQueue = new WorkQueue<>();

    PCWorkerPool<K, V, R> parentPool;

    public void loop() {
        while (true) {
            var poll = workQueue.poll();
            process(poll);
        }
    }

    public int getQueueCapacity(Timer workRetrievalTimer) {
        return calculateQuantityToGet(workRetrievalTimer) - workQueue.size();
    }

    private int calculateQuantityToGet(Timer workRetrievalTimer) {
        var retrieval = workRetrievalTimer.mean(NANOSECONDS);
        var processing = userFunctionTimer.mean(NANOSECONDS);
        var quantity = retrieval / processing;
        return (int) quantity * 2;
    }

    private void process(List<WorkContainer<K, V>> batch) {
        userFunctionTimer.record(() -> {
                    var functionRunner = parentPool.getRunner();
//                    if (functionRunner.isPresent()) {
//                        functionRunner.get().run(work);
                    functionRunner.run(batch);
//                    } else {
//                        throw new IllegalStateException("Function runner not set");
//                    }
                }
        );
    }

    public void enqueue(List<WorkContainer<K, V>> work) {
        workQueue.add(work);
    }
}

