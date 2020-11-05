package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Properties;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

/**
 * Basic core examples
 */
@Slf4j
public class CoreApp {

    static String inputTopic = "input-topic-" + RandomUtils.nextInt();
    static String outputTopic = "output-topic-" + RandomUtils.nextInt();

    Consumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    ParallelStreamProcessor<String, String> parallelConsumer;

    @SuppressWarnings("UnqualifiedFieldAccess")
    void run() {
        this.parallelConsumer = setupConsumer();

        // tag::example[]
        parallelConsumer.register(record ->
                log.info("Concurrently processing a record: {}", record)
        );
        // end::example[]
    }

    @SuppressWarnings({"FeatureEnvy", "MagicNumber"})
    ParallelStreamProcessor<String, String> setupConsumer() {
        // tag::exampleSetup[]
        var options = ParallelConsumerOptions.builder()
                .ordering(KEY) // <1>
                .maxConcurrency(1000) // <2>
                .maxUncommittedMessagesToHandlePerPartition(10000) // <3>
                .build();

        Consumer<String, String> kafkaConsumer = getKafkaConsumer(); // <4>
        if (!(kafkaConsumer instanceof MockConsumer)) {
            kafkaConsumer.subscribe(UniLists.of(inputTopic)); // <5>
        }

        return ParallelStreamProcessor.createEosStreamProcessor(kafkaConsumer, getKafkaProducer(), options);
        // end::exampleSetup[]
    }

    void close() {
        this.parallelConsumer.close();
    }

    void runPollAndProduce() {
        this.parallelConsumer = setupConsumer();

        // tag::exampleProduce[]
        this.parallelConsumer.register(record -> {
                    var result = processBrokerRecord(record);
                    ProducerRecord<String, String> produceRecord =
                            new ProducerRecord<>(outputTopic, "a-key", result.payload);
                    return UniLists.of(produceRecord);
                }, consumeProduceResult ->
                        log.info("Message {} saved to broker at offset {}",
                                consumeProduceResult.getOut(),
                                consumeProduceResult.getMeta().offset())
        );
        // end::exampleProduce[]
    }

    private Result processBrokerRecord(ConsumerRecord<String, String> record) {
        return new Result("Some payload from " + record.value());
    }

    @Value
    static class Result {
        String payload;
    }

}
