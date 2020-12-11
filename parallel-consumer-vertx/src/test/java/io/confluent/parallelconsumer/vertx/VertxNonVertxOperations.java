package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTest;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;

/**
 * Ensure all plain operations from {@link ParallelEoSStreamProcessorTest} still work with the extended vertx consumer
 */
public class VertxNonVertxOperations extends ParallelEoSStreamProcessorTest {

    @Override
    protected ParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        parallelConsumer = new VertxParallelEoSStreamProcessor<>(vertx, WebClient.create(vertx), parallelConsumerOptions);

        return parallelConsumer;
    }

}
