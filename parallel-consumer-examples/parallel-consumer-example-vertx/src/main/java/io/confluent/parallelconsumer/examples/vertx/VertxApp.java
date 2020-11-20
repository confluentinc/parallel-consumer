package io.confluent.parallelconsumer.examples.vertx;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.vertx.JStreamVertxParallelStreamProcessor;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor.RequestInfo;
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

@Slf4j
public class VertxApp {

    static String inputTopic = "input-topic-" + RandomUtils.nextInt();

    Consumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    JStreamVertxParallelStreamProcessor<String, String> parallelConsumer;


    void run() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        Producer<String, String> kafkaProducer = getKafkaProducer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .maxConcurrency(1000)
                .maxUncommittedMessagesToHandle(10000)
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        setupSubscription(kafkaConsumer);

        this.parallelConsumer = JStreamVertxParallelStreamProcessor.createEosStreamProcessor(options);

        int port = getPort();

        // tag::example[]
        var resultStream = parallelConsumer.vertxHttpReqInfoStream(record -> {
            log.info("Concurrently constructing and returning RequestInfo from record: {}", record);
            Map<String, String> params = UniMaps.of("recordKey", record.key(), "payload", record.value());
            return new RequestInfo("localhost", port, "/api", params); // <1>
        });
        // end::example[]

        resultStream.forEach(x -> {
            log.info("From result stream: {}", x);
        });

    }

    protected int getPort() {
        return 8080;
    }

    void setupSubscription(Consumer<String, String> kafkaConsumer) {
        kafkaConsumer.subscribe(UniLists.of(inputTopic));
    }

    void close() {
        this.parallelConsumer.closeDrainFirst(Duration.ofSeconds(2));
    }

}
