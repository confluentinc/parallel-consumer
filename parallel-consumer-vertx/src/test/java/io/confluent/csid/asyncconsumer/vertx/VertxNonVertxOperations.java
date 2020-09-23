package io.confluent.csid.asyncconsumer.vertx;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.asyncconsumer.ParallelConsumer;
import io.confluent.csid.asyncconsumer.ParallelConsumerOptions;
import io.confluent.csid.asyncconsumer.ParallelConsumerTest;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;

/**
 * Ensure all plain operations still work with the extended vertx consumer
 */
public class VertxNonVertxOperations extends ParallelConsumerTest {

    @Override
    protected ParallelConsumer initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        parallelConsumer = new VertxParallelConsumer<>(consumerSpy, producerSpy, vertx, WebClient.create(vertx), parallelConsumerOptions);

        return parallelConsumer;
    }

}
