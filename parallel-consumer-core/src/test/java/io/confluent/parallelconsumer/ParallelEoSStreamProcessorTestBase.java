package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParallelEoSStreamProcessorTestBase extends AbstractParallelEoSStreamProcessorTestBase {

    protected ParallelEoSStreamProcessor<String, String> parallelConsumer;

    @Override
    protected AbstractParallelEoSStreamProcessor<String, String> initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        return initPollingAsyncConsumer(parallelConsumerOptions);
    }

    protected ParallelEoSStreamProcessor<String, String> initPollingAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        parallelConsumer = new ParallelEoSStreamProcessor<>(parallelConsumerOptions);
        super.parentParallelConsumer = parallelConsumer;
        return parallelConsumer;
    }

}
