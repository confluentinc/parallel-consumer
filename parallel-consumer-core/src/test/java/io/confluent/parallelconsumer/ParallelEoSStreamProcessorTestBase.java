package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.PCModule;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class ParallelEoSStreamProcessorTestBase extends AbstractParallelEoSStreamProcessorTestBase {

    protected ParallelEoSStreamProcessor<String, String> parallelConsumer;

    @Override
    protected AbstractParallelEoSStreamProcessor<String, String> initParallelConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        return initPollingParallelConsumer(parallelConsumerOptions);
    }

    protected ParallelEoSStreamProcessor<String, String> initPollingParallelConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        PCModule module = createModule(parallelConsumerOptions);
        parallelConsumer = module == null ?
                new ParallelEoSStreamProcessor<>(parallelConsumerOptions) :
                new ParallelEoSStreamProcessor<>(parallelConsumerOptions, module);
        super.parentParallelConsumer = parallelConsumer;
        return parallelConsumer;
    }

    protected PCModule<String, String> createModule(final ParallelConsumerOptions parallelConsumerOptions) {
        return null;
    }

}
