package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Basic core examples
 */
@Slf4j
public class CoreApp {


    String inputTopic = "input-topic-" + RandomUtils.nextInt();
    String outputTopic = "output-topic-" + RandomUtils.nextInt();

    Consumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    ParallelStreamProcessor<String, String> parallelConsumer;

    @SuppressWarnings("UnqualifiedFieldAccess")
    void run() {
        this.parallelConsumer = setupParallelConsumer();

        postSetup();

        // tag::example[]
        parallelConsumer.poll(record -> {
                    log.info("Concurrently processing a record: {}", record);

                }
        );
        // end::example[]
    }

    protected void postSetup() {
        // ignore
    }

    @SuppressWarnings({"FeatureEnvy", "MagicNumber"})
    ParallelStreamProcessor<String, String> setupParallelConsumer() {
        // tag::exampleSetup[]
        Consumer<String, String> kafkaConsumer = getKafkaConsumer(); // <1>
        Producer<String, String> kafkaProducer = getKafkaProducer();

        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(KEY) // <2>
                .maxConcurrency(1000) // <3>
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        ParallelStreamProcessor<String, String> eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        eosStreamProcessor.subscribe(of(inputTopic)); // <4>

        return eosStreamProcessor;
        // end::exampleSetup[]
    }

    void close() {
        this.parallelConsumer.close();
    }

    void runPollAndProduce() {
        this.parallelConsumer = setupParallelConsumer();

        postSetup();

        // tag::exampleProduce[]
        parallelConsumer.pollAndProduce(record -> {
                    var result = processBrokerRecord(record);
                    return new ProducerRecord<>(outputTopic, record.key(), result.payload);
                }, consumeProduceResult -> {
                    log.debug("Message {} saved to broker at offset {}",
                            consumeProduceResult.getOut(),
                            consumeProduceResult.getMeta().offset());
                }
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

    void customRetryDelay() {
        // tag::customRetryDelay[]
        ParallelConsumerOptions.<String, String>builder()
                .retryDelayProvider(workContainer -> {
                    int numberOfFailedAttempts = workContainer.getNumberOfFailedAttempts();
                    final double multiplier = 0.5;
                    final int baseDelaySecond = 1;
                    long delaySeconds = (long) (baseDelaySecond * Math.pow(multiplier, numberOfFailedAttempts));
                    return Duration.ofSeconds(delaySeconds);
                });
        // end::customRetryDelay[]
    }


    void maxRetries() {
        ParallelStreamProcessor<String, String> pc = ParallelStreamProcessor.createEosStreamProcessor(null);
        // tag::maxRetries[]
        final int maxRetries = 0;
        final Map<ConsumerRecord<String, String>, Long> retriesCount = new ConcurrentHashMap<>();

        pc.poll(consumerRecord -> {
            Long retryCount = retriesCount.computeIfAbsent(consumerRecord, ignore -> 0L);
            if (retryCount < maxRetries) {
                processRecord(consumerRecord);
                // no exception, so completed - remove from map
                retriesCount.remove(consumerRecord);
            } else {
                log.warn("Retry count {} exceed manx of {} for record {}", retryCount, maxRetries, consumerRecord);
                // giving up, remove from map
                retriesCount.remove(consumerRecord);
            }
        });
        // tag::maxRetries[]
    }

    private void processRecord(final ConsumerRecord<String, String> record) {
        // no-op
    }

}
