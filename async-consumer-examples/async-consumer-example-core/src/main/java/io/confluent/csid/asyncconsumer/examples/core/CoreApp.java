package io.confluent.csid.asyncconsumer.examples.core;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.asyncconsumer.AsyncConsumer;
import io.confluent.csid.asyncconsumer.AsyncConsumerOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Properties;

import static io.confluent.csid.asyncconsumer.AsyncConsumerOptions.ProcessingOrder.KEY;

/**
 * Basic core examples
 */
@Slf4j
public class CoreApp {

    static String inputTopic = "input-topic-" + RandomUtils.nextInt();

    Consumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    AsyncConsumer<String, String> async;

    void run() {
        async = setupAsync();

        async.asyncPoll(record -> {
            log.info("Concurrently processing a record: {}", record);
        });
    }

    AsyncConsumer<String, String> setupAsync() {
        var options = AsyncConsumerOptions.builder()
                .ordering(KEY)
                .maxConcurrency(1000)
                .maxUncommittedMessagesToHandle(10000)
                .build();

        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        setupSubscription(kafkaConsumer);

        return new AsyncConsumer<>(kafkaConsumer, getKafkaProducer(), options);
    }

    void setupSubscription(Consumer<String, String> kafkaConsumer) {
        kafkaConsumer.subscribe(UniLists.of(inputTopic));
    }

    void close() {
        async.close();
    }

}
