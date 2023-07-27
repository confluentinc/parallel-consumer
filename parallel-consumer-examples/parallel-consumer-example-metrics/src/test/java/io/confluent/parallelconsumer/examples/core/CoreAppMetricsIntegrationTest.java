package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
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
                UniLists.of("pc_status","pc_partitions_number","pc_incomplete_offsets_total","pc_user_function_processing_time_seconds");


        coreApp.run();

        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 0, "a key 1", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 1, "a key 2", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.inputTopic, 0, 2, "a key 3", "a value"));


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
        @Override
        Consumer<String, String> getKafkaConsumer() {
            when(mockConsumer.groupMetadata())
                    .thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer
            return mockConsumer;
        }

        @Override
        protected void postSetup() {
            super.postSetup();

            mockConsumer.subscribeWithRebalanceAndAssignment(of(inputTopic), 1);
        }
    }
}