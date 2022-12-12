package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import pl.tlinkowski.unij.api.UniLists;

import java.io.BufferedInputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.when;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
@Testcontainers
public class CoreAppMetricsIntegrationTest{

    @Container
    final static private PrometheusContainer PROMETHEUS_CONTAINER = new PrometheusContainer();

    @Test
    @SneakyThrows
    void testMetrics() {
        org.testcontainers.Testcontainers.exposeHostPorts(7001);
        CoreAppUnderTest coreApp = new CoreAppUnderTest();

        final var expectedMetrics =
                UniLists.of("pc_status","pc_partitions","pc_incomplete_offsets_total","pc_user_function_processing_time_seconds");


        coreApp.run();

        Awaitility.await().pollDelay(Duration.ofSeconds(1)).untilAsserted(() -> {
            final var metrics = getPrometheusMetrics();
            Assert.assertTrue(metrics.containsAll(expectedMetrics));
        });

        coreApp.close();
    }

    @SneakyThrows
    private Set<String> getPrometheusMetrics(){
        ObjectMapper mapper = new ObjectMapper();

        final var url = new URL(String.format("%s/api/v1/metadata", PROMETHEUS_CONTAINER.getPrometheusEndpoint()));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        Assert.assertEquals(conn.getResponseCode(), 200);

        final Map<String, Object> jsonBody = mapper.readValue(new BufferedInputStream(conn.getInputStream()), Map.class);
        return ((Map) jsonBody.get("data")).keySet();
    }

    class CoreAppUnderTest extends CoreApp {
        LongPollingMockConsumer<String, String> mockConsumer = Mockito.spy(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));
        TopicPartition tp = new TopicPartition(inputTopic, 0);
        Consumer<String, String> consumer = null;

        @Override
        Consumer<String, String> getKafkaConsumer() {
            when(mockConsumer.groupMetadata())
                    .thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer
            return mockConsumer;
        }

        @Override
        Producer<String, String> getKafkaProducer() {
            var stringSerializer = Serdes.String().serializer();
            return new MockProducer<>(true, stringSerializer, stringSerializer);
        }

        @Override
        protected void postSetup() {
            super.postSetup();

            mockConsumer.subscribeWithRebalanceAndAssignment(of(inputTopic), 1);
        }
    }
}
