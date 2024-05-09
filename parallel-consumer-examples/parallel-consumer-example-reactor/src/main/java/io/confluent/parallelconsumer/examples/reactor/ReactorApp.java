package io.confluent.parallelconsumer.examples.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
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

        // tag::example[]
        parallelConsumer.react(context -> {
            var consumerRecord = context.getSingleRecord().getConsumerRecord();
            log.info("Concurrently constructing and returning RequestInfo from record: {}", consumerRecord);
            Map<String, String> params = UniMaps.of("recordKey", consumerRecord.key(), "payload", consumerRecord.value());
            return Mono.just("something todo"); // <1>
        });
        // end::example[]
    }

    void close() {
        this.parallelConsumer.closeDrainFirst();
    }

    protected void postSetup() {
        // no-op, for testing
    }

}
