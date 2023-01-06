package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.sun.net.httpserver.HttpServer;
import io.confluent.parallelconsumer.PCMetricsTracker;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Basic core examples
 */
@Slf4j
@NoArgsConstructor
public class CoreApp {
    public static final String METRICS_ENDPOINT = "/prometheus";
    final PrometheusMeterRegistry metricsRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    String inputTopic = "input-topic";
    String outputTopic = "output-topic-" + RandomUtils.nextInt();
    PCMetricsTracker pcMetricsTracker;
    private final Map<String, String> envVars = System.getenv();

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
            server.createContext(METRICS_ENDPOINT, httpExchange -> {
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

        parallelConsumer.poll(record ->{
            log.info("Concurrently processing a record: {}", record);
        });
    }

    protected void postSetup() {
        this.setupPrometheusEndpoint();
    }

    @SuppressWarnings({"FeatureEnvy", "MagicNumber"})
    ParallelStreamProcessor<String, String> setupParallelConsumer() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer(); // <1>

        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY) // <2>
                .maxConcurrency(1000) // <3>
                .consumer(kafkaConsumer)
                .meterRegistry(metricsRegistry)
                .build();

        ParallelStreamProcessor<String, String> eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        eosStreamProcessor.subscribe(of(inputTopic)); // <4>

        new KafkaClientMetrics(kafkaConsumer).bindTo(metricsRegistry);

        pcMetricsTracker = new PCMetricsTracker(eosStreamProcessor::calculateMetricsWithIncompletes,
                UniLists.of(Tag.of("region", "eu-west-1"), Tag.of("instance", "pc1")));

        eosStreamProcessor.registerMetricsTracker(pcMetricsTracker);


        return eosStreamProcessor;
    }

    void close() {
        this.parallelConsumer.close();
        this.pcMetricsTracker.close();
        this.metricsEndpointExecutor.shutdownNow();
    }
}