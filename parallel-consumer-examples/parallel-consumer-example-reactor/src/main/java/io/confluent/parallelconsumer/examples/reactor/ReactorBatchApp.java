package io.confluent.parallelconsumer.examples.reactor;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.reactor.ReactorProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import reactor.core.publisher.Mono;

import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class ReactorBatchApp {

    static String inputTopic = "input-topic-" + RandomUtils.nextInt();
    static int batchSize = 5;
    static int minBatchSize = 3;
    protected int minBatchTimeout = 100;


    Consumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    ReactorProcessor<String, String> parallelConsumer;

    CopyOnWriteArrayList<Integer> batchSizes = new CopyOnWriteArrayList<>();


    void run() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        Producer<String, String> kafkaProducer = getKafkaProducer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .batchSize(batchSize)
                .minBatchSize(minBatchSize)
                .minBatchTimeoutInMillis(minBatchTimeout)
                .maxConcurrency(1000)
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        this.parallelConsumer = new ReactorProcessor<>(options);
        parallelConsumer.subscribe(of(inputTopic));

        postSetup();

        // tag::example[]
        parallelConsumer.react(context -> {
            var consumerRecords = context.getConsumerRecordsFlattened();
            log.info("Got a batch size of : {}", consumerRecords.size());
            batchSizes.add(consumerRecords.size());
            for(ConsumerRecord<String, String> consumerRecord: consumerRecords){
                log.info("Concurrently constructing and returning RequestInfo from record: {}", consumerRecord);
            }

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
