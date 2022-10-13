package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class ParallelEoSStreamProcessorTestBase extends AbstractParallelEoSStreamProcessorTestBase {

    protected ParallelEoSStreamProcessor<String, String> parallelConsumer;

    @Override
    protected AbstractParallelEoSStreamProcessor<String, String> initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        return initPollingAsyncConsumer(parallelConsumerOptions);
    }

    protected ParallelEoSStreamProcessor<String, String> initPollingAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        PCModuleTestEnv module = getModule();
        parallelConsumer = module == null ?
                new ParallelEoSStreamProcessor<>(parallelConsumerOptions) :
                new ParallelEoSStreamProcessor<>(parallelConsumerOptions, module);
        super.parentParallelConsumer = parallelConsumer;
        return parallelConsumer;
    }

    protected PCModuleTestEnv getModule() {
        return null;
    }

}
