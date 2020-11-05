package io.confluent.parallelconsumer;

import com.netflix.concurrency.limits.executors.BlockingAdaptiveExecutor;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class AutoScalingProcessor<K,V> extends ParallelEoSStreamProcessor<K,V> {

    private SimpleLimiter<Void> executionLimitor;
    private BlockingAdaptiveExecutor congestionControlledExecutor;

    public AutoScalingProcessor(org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                                      org.apache.kafka.clients.producer.Producer<K, V> producer,
                                      ParallelConsumerOptions options) {
        super(consumer, producer, options);
    }

    /**
     * Could move this into the builder pattern to keep the final modifier on the fields
     */
    @Override
    protected void constructExecutor(final ParallelConsumerOptions options) {
        executionLimitor = SimpleLimiter.newBuilder().limit(Gradient2Limit.newDefault()).build();
        super.workerPool = BlockingAdaptiveExecutor.newBuilder().limiter(executionLimitor).build();
    }

    protected <R> void getWorkAndRegister(final Function<ConsumerRecord<K, V>, List<R>> userFunction, final Consumer<R> callback) {
        int capacity = executionLimitor.getLimit() - executionLimitor.getInflight();
        boolean spareCapacity = capacity > 0;
        if (spareCapacity) {
            var records = wm.<R>maybeGetWork(capacity);
            log.trace("Loop: Submit to pool");
            submitWorkToPool(userFunction, callback, records);
        }
    }
}
