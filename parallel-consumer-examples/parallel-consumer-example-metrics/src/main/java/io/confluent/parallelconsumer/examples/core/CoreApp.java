package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import com.sun.net.httpserver.HttpServer;
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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
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
    final PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    String inputTopic = "input-topic";
    String outputTopic = "output-topic-" + RandomUtils.nextInt();
    private final Map<String, String> envVars = System.getenv();

    private KafkaClientMetrics kafkaClientMetrics;

    Consumer<String, String> getKafkaConsumer() {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envVars.getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, envVars.getOrDefault("GROUP_ID", "pc-instance"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(props);
    }

    ParallelStreamProcessor<String, String> parallelConsumer;

    private final ExecutorService metricsEndpointExecutor = Executors.newSingleThreadExecutor();

    void setupPrometheusEndpoint() {
        try {
            final var server = HttpServer.create(new InetSocketAddress(7001), 0);
            server.createContext(METRICS_ENDPOINT, httpExchange -> {
                String response = meterRegistry.scrape();
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

        parallelConsumer.poll(record -> {
            log.info("Concurrently processing a record: {}", record);
        });
    }

    protected void postSetup() {
        this.setupPrometheusEndpoint();
    }

    @SuppressWarnings({"FeatureEnvy", "MagicNumber" })
    ParallelStreamProcessor<String, String> setupParallelConsumer() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();

        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .maxConcurrency(1000)
                .consumer(kafkaConsumer)
                .meterRegistry(meterRegistry)                     //<1>
                .metricsTags(List.of(Tag.of("instance", "pc1")))    //<2>
                .build();

        ParallelStreamProcessor<String, String> eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        eosStreamProcessor.subscribe(of(inputTopic));

        kafkaClientMetrics = new KafkaClientMetrics(kafkaConsumer); //<3>
        kafkaClientMetrics.bindTo(meterRegistry);                 //<4>
        return eosStreamProcessor;
    }

    void close() {
        this.kafkaClientMetrics.close();
        this.parallelConsumer.close();
        this.metricsEndpointExecutor.shutdownNow();

    }
}