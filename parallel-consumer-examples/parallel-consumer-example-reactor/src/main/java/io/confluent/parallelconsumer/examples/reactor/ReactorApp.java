package io.confluent.parallelconsumer.examples.reactor;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.reactor.ReactorProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import pl.tlinkowski.unij.api.UniMaps;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Properties;

import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class ReactorApp {

    static String inputTopic = "input-topic-" + RandomUtils.nextInt();

    Consumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    ReactorProcessor<String, String> parallelConsumer;


    void run() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        Producer<String, String> kafkaProducer = getKafkaProducer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        this.parallelConsumer = new ReactorProcessor<>(options);
        parallelConsumer.subscribe(of(inputTopic));

        postSetup();

        int port = getPort();

        // tag::example[]
        parallelConsumer.react(record -> {
            log.info("Concurrently constructing and returning RequestInfo from record: {}", record);
            Map<String, String> params = UniMaps.of("recordKey", record.key(), "payload", record.value());
            return Mono.just("something todo"); // <1>
        });
        // end::example[]
    }

    protected int getPort() {
        return 8080;
    }

    void close() {
        this.parallelConsumer.closeDrainFirst();
    }

    protected void postSetup() {
        // no-op, for testing
    }

}
