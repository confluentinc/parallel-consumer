package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParallelEoSStreamProcessorTestBase extends AbstractParallelEoSStreamProcessorTestBase {

    protected ParallelEoSStreamProcessor<String, String> parallelConsumer;

    @Override
    protected ParentParallelEoSStreamProcessor<String, String> initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        parallelConsumer = initPollingAsyncConsumer(parallelConsumerOptions);
        return parallelConsumer;
    }

    protected ParallelEoSStreamProcessor<String, String> initPollingAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        parallelConsumer = new ParallelEoSStreamProcessor<>(parallelConsumerOptions);
        return parallelConsumer;
    }

}
