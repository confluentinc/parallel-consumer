package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.State;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * @author Nacho Munoz
 * @see PCMetricsTracker
 */
@Slf4j
class PCMetricsTrackerTest extends ParallelEoSStreamProcessorTestBase {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private final List<Tag> commonTags = UniLists.of(Tag.of("instance", "pc1"));

    @Test
    void metricsRegisterBinding() {
        final int quantity = 10_000;
        final var pcMetricsTracker = new PCMetricsTracker(this.parallelConsumer::calculateMetricsWithIncompletes, commonTags);

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
            assertEquals(State.running.ordinal(),
                    registeredValueFor(PCMetricsTracker.METRIC_NAME_PC_STATUS, "status", State.running.name()));
            assertEquals(1, registeredValueFor(PCMetricsTracker.METRIC_NAME_NUMBER_SHARDS));
            assertEquals(2, registeredValueFor(PCMetricsTracker.METRIC_NAME_NUMBER_PARTITIONS));
        });

        // metrics show processing is complete
        await().untilAsserted(() -> {
            log.info(registry.getMetersAsString());
            assertEquals(0, registeredValueFor(PCMetricsTracker.METRIC_NAME_INCOMPLETE_OFFSETS,
                    "topic", topicPartition.topic(), "partition", String.valueOf(topicPartition.partition())));

            assertEquals(quantity - 1, registeredValueFor(PCMetricsTracker.METRIC_NAME_HIGHEST_COMPLETED_OFFSET,
                    "topic", topicPartition.topic(), "partition", String.valueOf(topicPartition.partition())));

            assertEquals(0, registeredValueFor(PCMetricsTracker.METRIC_NAME_TOTAL_INCOMPLETE_OFFSETS));
        });
    }

    private double registeredValueFor(String name, String... filterTags) {
        return this.registry.get(name).tags(filterTags).gauge().value();
    }
}
