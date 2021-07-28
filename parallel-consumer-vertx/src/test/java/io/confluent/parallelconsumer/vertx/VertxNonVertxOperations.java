package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTest;

/**
 * Ensure all plain operations from {@link ParallelEoSStreamProcessorTest} still work with the extended vertx consumer
 */
public class VertxNonVertxOperations extends ParallelEoSStreamProcessorTest {

    // todo delete - invalid test
//    @Override
//    protected ParentParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
//        VertxOptions vertxOptions = new VertxOptions();
//        Vertx vertx = Vertx.vertx(vertxOptions);
//        parallelConsumer = new VertxParallelEoSStreamProcessor<>(vertx, WebClient.create(vertx), parallelConsumerOptions);
//        return parallelConsumer;
//    }

}
