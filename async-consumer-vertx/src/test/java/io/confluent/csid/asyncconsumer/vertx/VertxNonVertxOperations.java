package io.confluent.csid.asyncconsumer.vertx;

import io.confluent.csid.asyncconsumer.AsyncConsumerOptions;
import io.confluent.csid.asyncconsumer.AsyncConsumerTest;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import org.junit.jupiter.api.BeforeEach;

import static java.time.Duration.ofMillis;

/**
 * Ensure all plain operations still work with the extended vertx consumer
 */
public class VertxNonVertxOperations extends AsyncConsumerTest {

    @BeforeEach
    public void setupVertxNonVertxOperations() {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        asyncConsumer = new VertxAsyncConsumer<MyKey, MyInput>(consumerSpy, producerSpy, vertx, WebClient.create(vertx), AsyncConsumerOptions.builder().build());
        asyncConsumer.setLongPollTimeout(ofMillis(100));
        asyncConsumer.setTimeBetweenCommits(ofMillis(100));
    }

}
