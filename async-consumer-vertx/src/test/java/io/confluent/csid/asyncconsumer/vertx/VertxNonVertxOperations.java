package io.confluent.csid.asyncconsumer.vertx;

import io.confluent.csid.asyncconsumer.AsyncConsumer;
import io.confluent.csid.asyncconsumer.AsyncConsumerOptions;
import io.confluent.csid.asyncconsumer.AsyncConsumerTest;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;

/**
 * Ensure all plain operations still work with the extended vertx consumer
 */
public class VertxNonVertxOperations extends AsyncConsumerTest {

    @Override
    protected AsyncConsumer initAsyncConsumer(AsyncConsumerOptions asyncConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        asyncConsumer = new VertxAsyncConsumer<>(consumerSpy, producerSpy, vertx, WebClient.create(vertx), asyncConsumerOptions);

        return asyncConsumer;
    }

}
