package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.State;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nacho Munoz
 * @see PCMetricsTracker
 */
@Slf4j
class PCMetricsTrackerTest extends ParallelEoSStreamProcessorTestBase {
    private SimpleMeterRegistry registry;
    private final List<Tag> commonTags = UniLists.of(Tag.of("instance", "pc1"));

    @Test
    void metricsRegisterBinding() {
        registry = (SimpleMeterRegistry) this.getModule().meterRegistry();
        final int quantity = 10_000;
        final var pcMetricsTracker = new PCMetricsTracker(this.parallelConsumer::calculateMetricsWithIncompletes, commonTags);
        parallelConsumer.registerMetricsTracker(pcMetricsTracker);
        ktu.sendRecords(quantity);

        parallelConsumer.poll(recordContexts -> {
            recordContexts.forEach(recordContext -> {
                log.trace("Processing: {}", recordContext);
            });
        });

        pcMetricsTracker.bindTo(registry);
        // metrics have some data
        await().untilAsserted(() -> {
            assertFalse(registry.getMeters().isEmpty());
            assertEquals(State.RUNNING.ordinal(),
                    registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_PC_STATUS, "status", State.RUNNING.name()));
            assertEquals(1, registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_NUMBER_SHARDS));
            assertEquals(2, registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_NUMBER_PARTITIONS));
        });

        // metrics show processing is complete
        await().untilAsserted(() -> {
            log.info(registry.getMetersAsString());
            assertEquals(0, registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_INCOMPLETE_OFFSETS,
                    "topic", topicPartition.topic(), "partition", String.valueOf(topicPartition.partition())));

            assertEquals(quantity - 1, registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_HIGHEST_COMPLETED_OFFSET,
                    "topic", topicPartition.topic(), "partition", String.valueOf(topicPartition.partition())));

            assertEquals(0, registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_TOTAL_INCOMPLETE_OFFSETS));

            assertEquals(10_000, registeredCounterValueFor(PCMetricsTracker.METRIC_NAME_PROCESSED_RECORDS,
                    "epoch", "0", "topic", topicPartition.topic(), "partition", String.valueOf(topicPartition.partition())));
            assertTrue(0.0 < registeredTimerFor(PCMetricsTracker.METRIC_NAME_USER_FUNCTION_PROCESSING_TIME));
        });
    }

    private double registeredGaugeValueFor(String name, String... filterTags) {
        return registry.get(name).tags(filterTags).gauge().value();
    }

    private double registeredCounterValueFor(String name, String... filterTags) {
        return Optional.ofNullable(registry.find(name).tags(filterTags).counter())
                .map(counter -> counter.count()).orElse(0.0);
    }
    private double registeredTimerFor(String metricName, String... tags) {
        return Optional.ofNullable(registry.find(metricName).tags(tags).timer())
                .map(timer -> timer.mean(TimeUnit.MILLISECONDS)).orElse(0.0);
    }
}
}
