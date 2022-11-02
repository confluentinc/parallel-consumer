package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Antony Stubbs
 * @see PCMetrics
 */
@Slf4j
class PCMetricsTest extends ParallelEoSStreamProcessorTestBase {

    @Test
    void metricsBasics() {
        final int quantity = 10_000;
        ktu.sendRecords(quantity);

        parallelConsumer.poll(recordContexts -> {
            recordContexts.forEach(recordContext -> {
                log.trace("Processing: {}", recordContext);
            });
        });

        {
            PCMetrics pcMetrics = parallelConsumer.calculateMetrics();
            assertThat(pcMetrics).getPartitionMetrics().isNotEmpty();
        }

        // metrics have some data
        await().untilAsserted(() -> {
            PCMetrics pcMetrics = parallelConsumer.calculateMetrics();
            PCMetrics.PCPartitionMetrics pcPartitionMetrics = pcMetrics.getPartitionMetrics().get(topicPartition);
            assertThat(pcPartitionMetrics).getHighestSeenOffset().isAtLeast(400L);
            assertThat(pcPartitionMetrics).getHighestCompletedOffset().isAtLeast(1L);
            assertThat(pcPartitionMetrics).getNumberOfIncompletes().isEqualTo(0);
        });

        final SimpleMeterRegistry metricsRegistry = parallelConsumer.getMetricsRegistry();
        metricsRegistry.getMeters().forEach(meter -> {
            log.error("Meter: {}", meter);
        });

        // metrics show processing is complete
        await().untilAsserted(() -> {
            PCMetrics pcMetrics = parallelConsumer.calculateMetricsWithIncompletes();
            PCMetrics.PCPartitionMetrics pcPartitionMetrics = pcMetrics.getPartitionMetrics().get(topicPartition);
            assertThat(pcPartitionMetrics).getHighestCompletedOffset().isEqualTo(quantity - 1);
            assertThat(pcPartitionMetrics).getNumberOfIncompletes().isEqualTo(0);
            assertThat(pcMetrics).getTotalNumberOfIncompletes().isEqualTo(0);
            var incompleteMetrics = pcPartitionMetrics.getIncompleteMetrics();
            assertThat(incompleteMetrics).isPresent();
            var incompletes = incompleteMetrics.get().getIncompleteOffsets();
            assertThat(incompletes).isEmpty();
        });

        {
            PCMetrics metrics = parallelConsumer.calculateMetricsWithIncompletes();
            log.warn("timer: {} ms", metrics.getFunctionTimer().mean(TimeUnit.MILLISECONDS));
            log.warn("Counter: {}", metrics.getSuccessCounter().count());
        }
    }
}
