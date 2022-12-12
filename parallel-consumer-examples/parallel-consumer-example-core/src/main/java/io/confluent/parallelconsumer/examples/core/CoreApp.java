package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.sun.net.httpserver.HttpServer;
import io.confluent.parallelconsumer.*;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.concurrent.CircuitBreakingException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.StringUtils.msg;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Basic core examples
 */
@Slf4j
public class CoreApp {
    final PrometheusMeterRegistry metricsRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    String inputTopic = "input-topic";
    String outputTopic = "output-topic-" + RandomUtils.nextInt();
    PCMetricsTracker pcMetricsTracker;
    private final Map<String, String> envVars = System.getenv();

    public CoreApp() {
    }

    Consumer<String, String> getKafkaConsumer() {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,envVars.getOrDefault("BOOTSTRAP_SERVERS","kafka:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG,envVars.getOrDefault("GROUP_ID","pc-instance"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(props);
    }

    Producer<String, String> getKafkaProducer() {
        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,envVars.getOrDefault("BOOTSTRAP_SERVERS","kafka:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }

    ParallelStreamProcessor<String, String> parallelConsumer;

    private final ExecutorService metricsEndpointExecutor = Executors.newSingleThreadExecutor();

    void setupPrometheusEndpoint(){
        try {
            final var server = HttpServer.create(new InetSocketAddress(7001), 0);
            server.createContext("/prometheus", httpExchange -> {
                String response = metricsRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            metricsEndpointExecutor.submit(server::start);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @SuppressWarnings("UnqualifiedFieldAccess")
    void run() {
        this.parallelConsumer = setupParallelConsumer();
        postSetup();

        // tag::example[]
        parallelConsumer.poll(record ->{
            if (RandomUtils.nextInt() % 3 == 0)
                throw new PCRetriableException();
            log.info("Concurrently processing a record: {}", record);
        });
        // end::example[]
    }

    protected void postSetup() {
        this.setupPrometheusEndpoint();
    }

    @SuppressWarnings({"FeatureEnvy", "MagicNumber"})
    ParallelStreamProcessor<String, String> setupParallelConsumer() {
        // tag::exampleSetup[]
        Consumer<String, String> kafkaConsumer = getKafkaConsumer(); // <1>
        Producer<String, String> kafkaProducer = getKafkaProducer();

        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED) // <2>
                .maxConcurrency(1000) // <3>
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .meterRegistry(metricsRegistry)
                .build();

        ParallelStreamProcessor<String, String> eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        eosStreamProcessor.subscribe(of(inputTopic)); // <4>
        pcMetricsTracker = new PCMetricsTracker(((AbstractParallelEoSStreamProcessor)eosStreamProcessor)::calculateMetricsWithIncompletes,
                            UniLists.of(Tag.of("region", "eu-west-1"), Tag.of("instance", "pc1")));

        UniLists.of(pcMetricsTracker, new KafkaClientMetrics(kafkaConsumer), new JvmMemoryMetrics(),
                        new JvmHeapPressureMetrics(), new JvmGcMetrics(), new JvmThreadMetrics())
                    .stream().forEach(m -> m.bindTo(metricsRegistry));

        return eosStreamProcessor;
        // end::exampleSetup[]
    }

    void close() {
        this.parallelConsumer.close();
        this.pcMetricsTracker.close();
        this.metricsEndpointExecutor.shutdownNow();
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

    public static void main (String[] args) { new CoreApp().run(); }
}
