package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.PCModule;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParallelEoSStreamProcessorTestBase extends AbstractParallelEoSStreamProcessorTestBase {

    protected ParallelEoSStreamProcessor<String, String> parallelConsumer;

    @Getter
    private PCModule module;

    @Override
    protected AbstractParallelEoSStreamProcessor<String, String> initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        return initPollingAsyncConsumer(parallelConsumerOptions);
    }

    protected ParallelEoSStreamProcessor<String, String> initPollingAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        module = new PCModule<>(parallelConsumerOptions);
        parallelConsumer = new ParallelEoSStreamProcessor<>(parallelConsumerOptions, module);
        super.parentParallelConsumer = parallelConsumer;
        return parallelConsumer;
    }

}
