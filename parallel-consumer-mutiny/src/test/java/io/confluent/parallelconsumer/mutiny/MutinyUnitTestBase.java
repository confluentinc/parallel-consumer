package io.confluent.parallelconsumer.mutiny;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;

public class MutinyUnitTestBase extends ParallelEoSStreamProcessorTestBase {

    protected MutinyProcessor<String, String> mutinyPC;

    protected static final int MAX_CONCURRENCY = 1000;

    @Override
    protected AbstractParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        var build = parallelConsumerOptions.toBuilder()
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .maxConcurrency(MAX_CONCURRENCY)
                .build();

        mutinyPC = new MutinyProcessor<>(build);

        return mutinyPC;
    }
}