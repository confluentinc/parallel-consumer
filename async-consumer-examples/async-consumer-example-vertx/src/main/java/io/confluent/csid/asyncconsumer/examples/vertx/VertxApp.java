package io.confluent.csid.asyncconsumer.examples.vertx;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.asyncconsumer.AsyncConsumerOptions;
import io.confluent.csid.asyncconsumer.vertx.StreamingAsyncVertxConsumer;
import io.confluent.csid.asyncconsumer.vertx.VertxAsyncConsumer.RequestInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

@Slf4j
public class VertxApp {

    static String inputTopic = "input-topic-" + RandomUtils.nextInt();

    Consumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    StreamingAsyncVertxConsumer<String, String> async;


    void run() {
        var options = AsyncConsumerOptions.builder()
                .ordering(AsyncConsumerOptions.ProcessingOrder.KEY)
                .maxConcurrency(1000)
                .maxUncommittedMessagesToHandlePerPartition(10000)
                .build();

        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        setupSubscription(kafkaConsumer);

        async = new StreamingAsyncVertxConsumer<>(kafkaConsumer,
                getKafkaProducer(), options);

        // tag::example[]
        var resultStream = async.vertxHttpReqInfoStream(record -> {
            log.info("Concurrently constructing and returning RequestInfo from record: {}", record);
            Map params = UniMaps.of("recordKey", record.key(), "payload", record.value());
            return new RequestInfo("localhost", "/api", params); // <1>
        });
        // end::example[]

        resultStream.forEach(x->{
            log.info("From result stream: {}", x);
        });

    }

    void setupSubscription(Consumer<String, String> kafkaConsumer) {
        kafkaConsumer.subscribe(UniLists.of(inputTopic));
    }

    void close() {
        async.close(Duration.ofSeconds(2), true);
    }

}
