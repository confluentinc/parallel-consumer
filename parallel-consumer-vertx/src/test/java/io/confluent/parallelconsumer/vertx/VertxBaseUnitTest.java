package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import org.junit.jupiter.api.BeforeEach;

public abstract class VertxBaseUnitTest extends ParallelEoSStreamProcessorTestBase {

    JStreamVertxParallelEoSStreamProcessor<String, String> vertxAsync;

    @Override
    protected AbstractParallelEoSStreamProcessor initParallelConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        WebClient wc = WebClient.create(vertx);
        var build = parallelConsumerOptions.toBuilder()
                .maxConcurrency(10)
                .build();
        vertxAsync = new JStreamVertxParallelEoSStreamProcessor<>(vertx, wc, build);

        return vertxAsync;
    }

    @BeforeEach
    public void setupData() {
        super.sendOneRecord();
    }

}
