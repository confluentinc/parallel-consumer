package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.PCMetricsTracker;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.RecordContext;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.concurrent.CircuitBreakingException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Basic core examples
 */
@Slf4j
public class CoreApp {
    final MeterRegistry metricsRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    String inputTopic = "input-topic-" + RandomUtils.nextInt();
    String outputTopic = "output-topic-" + RandomUtils.nextInt();
    PCMetricsTracker pcMetricsTracker;
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
        parallelConsumer.poll(record ->
                log.info("Concurrently processing a record: {}", record)
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

        new KafkaClientMetrics(kafkaConsumer).bindTo(metricsRegistry);
        new KafkaClientMetrics(kafkaProducer).bindTo(metricsRegistry);

        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(KEY) // <2>
                .maxConcurrency(1000) // <3>
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        ParallelStreamProcessor<String, String> eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);


        eosStreamProcessor.subscribe(of(inputTopic)); // <4>
        pcMetricsTracker = new PCMetricsTracker(((AbstractParallelEoSStreamProcessor)eosStreamProcessor)::calculateMetricsWithIncompletes,
                            UniLists.of(Tag.of("region", "eu-west-1"), Tag.of("instance", "pc1")));
        pcMetricsTracker.bindTo(metricsRegistry);
        return eosStreamProcessor;
        // end::exampleSetup[]
    }

    void close() {
        this.parallelConsumer.close();
        this.pcMetricsTracker.close();
    }

    void runPollAndProduce() {
        this.parallelConsumer = setupParallelConsumer();

        postSetup();

        // tag::exampleProduce[]
        parallelConsumer.pollAndProduce(context -> {
                    var consumerRecord = context.getSingleRecord().getConsumerRecord();
                    var result = processBrokerRecord(consumerRecord);
                    return new ProducerRecord<>(outputTopic, consumerRecord.key(), result.payload);
                }, consumeProduceResult -> {
                    log.debug("Message {} saved to broker at offset {}",
                            consumeProduceResult.getOut(),
                            consumeProduceResult.getMeta().offset());
                }
        );
        // end::exampleProduce[]
    }

    private Result processBrokerRecord(ConsumerRecord<String, String> consumerRecord) {
        return new Result("Some payload from " + consumerRecord.value());
    }

    @Value
    static class Result {
        String payload;
    }

    void customRetryDelay() {
        // tag::customRetryDelay[]
        final double multiplier = 0.5;
        final int baseDelaySecond = 1;

        ParallelConsumerOptions.<String, String>builder()
                .retryDelayProvider(recordContext -> {
                    int numberOfFailedAttempts = recordContext.getNumberOfFailedAttempts();
                    long delayMillis = (long) (baseDelaySecond * Math.pow(multiplier, numberOfFailedAttempts) * 1000);
                    return Duration.ofMillis(delayMillis);
                });
        // end::customRetryDelay[]
    }


    void maxRetries() {
        ParallelStreamProcessor<String, String> pc = ParallelStreamProcessor.createEosStreamProcessor(null);
        // tag::maxRetries[]
        final int maxRetries = 10;
        final Map<ConsumerRecord<String, String>, Long> retriesCount = new ConcurrentHashMap<>();

        pc.poll(context -> {
            var consumerRecord = context.getSingleRecord().getConsumerRecord();
            Long retryCount = retriesCount.computeIfAbsent(consumerRecord, ignore -> 0L);
            if (retryCount < maxRetries) {
                processRecord(consumerRecord);
                // no exception, so completed - remove from map
                retriesCount.remove(consumerRecord);
            } else {
                log.warn("Retry count {} exceeded max of {} for record {}", retryCount, maxRetries, consumerRecord);
                // giving up, remove from map
                retriesCount.remove(consumerRecord);
            }
        });
        // end::maxRetries[]
    }

    private void processRecord(final ConsumerRecord<String, String> record) {
        // no-op
    }

    void circuitBreaker() {
        ParallelStreamProcessor<String, String> pc = ParallelStreamProcessor.createEosStreamProcessor(null);
        // tag::circuitBreaker[]
        final Map<String, Boolean> upMap = new ConcurrentHashMap<>();

        pc.poll(context -> {
            var consumerRecord = context.getSingleRecord().getConsumerRecord();
            String serverId = extractServerId(consumerRecord);
            boolean up = upMap.computeIfAbsent(serverId, ignore -> true);

            if (!up) {
                up = updateStatusOfSever(serverId);
            }

            if (up) {
                try {
                    processRecord(consumerRecord);
                } catch (CircuitBreakingException e) {
                    log.warn("Server {} is circuitBroken, will retry message when server is up. Record: {}", serverId, consumerRecord);
                    upMap.put(serverId, false);
                }
                // no exception, so set server status UP
                upMap.put(serverId, true);
            } else {
                throw new RuntimeException(msg("Server {} currently down, will retry record latter {}", up, consumerRecord));
            }
        });
        // end::circuitBreaker[]
    }

    private boolean updateStatusOfSever(final String serverId) {
        return false;
    }

    private String extractServerId(final ConsumerRecord<String, String> consumerRecord) {
        // no-op
        return null;
    }


    void batching() {
        // tag::batching[]
        ParallelStreamProcessor.createEosStreamProcessor(ParallelConsumerOptions.<String, String>builder()
                .consumer(getKafkaConsumer())
                .producer(getKafkaProducer())
                .maxConcurrency(100)
                .batchSize(5) // <1>
                .build());
        parallelConsumer.poll(context -> {
            // convert the batch into the payload for our processing
            List<String> payload = context.stream()
                    .map(this::preparePayload)
                    .collect(Collectors.toList());
            // process the entire batch payload at once
            processBatchPayload(payload);
        });
        // end::batching[]
    }

    private void processBatchPayload(List<String> batchPayload) {
        // example
    }

    private String preparePayload(RecordContext<String, String> rc) {
        ConsumerRecord<String, String> consumerRecords = rc.getConsumerRecord();
        int failureCount = rc.getNumberOfFailedAttempts();
        return msg("{}, {}", consumerRecords, failureCount);
    }

}
