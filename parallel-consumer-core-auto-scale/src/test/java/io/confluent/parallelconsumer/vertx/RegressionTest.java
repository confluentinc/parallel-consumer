package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.AutoScalingProcessor;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTest;

/**
 * Ensure all plain operations still work with the extended vertx consumer
 */
public class RegressionTest extends ParallelEoSStreamProcessorTest {

    @Override
    protected ParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        // tag::construct[]
        parallelConsumer = new AutoScalingProcessor<>(consumerSpy, producerSpy, parallelConsumerOptions);
        // end::construct[]
        return parallelConsumer;
    }

}
