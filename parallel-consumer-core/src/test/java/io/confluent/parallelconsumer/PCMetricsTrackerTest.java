package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.State;
import io.confluent.parallelconsumer.state.ShardKey;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.confluent.parallelconsumer.internal.State.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;
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
    @SneakyThrows
    void metricsRegisterBinding() {
        registry = (SimpleMeterRegistry) this.getModule().meterRegistry();
        final int quantityP0 = 1000;
        final int quantityP1 = 500;
        AtomicInteger numberToBlockAt = new AtomicInteger(200);
        final int p1StartingOffset = quantityP0;
        final var pcMetricsTracker = new PCMetricsTracker(this.parallelConsumer::calculateMetricsWithIncompletes, commonTags);
        parallelConsumer.registerMetricsTracker(pcMetricsTracker);
        ktu.send(consumerSpy, ktu.generateRecords(0, quantityP0));
        ktu.send(consumerSpy, ktu.generateRecords(1, quantityP1));
        CountDownLatch latchPartition0 = new CountDownLatch(1);
        CountDownLatch latchPartition1 = new CountDownLatch(1);
        AtomicInteger counterP0 = new AtomicInteger();
        AtomicInteger counterP1 = new AtomicInteger();
        AtomicBoolean failedRecordDone = new AtomicBoolean(false);
        parallelConsumer.poll(recordContexts -> {
            recordContexts.forEach(recordContext -> {
                log.trace("Processing: {}", recordContext);
                try {
                    AtomicInteger counter;
                    CountDownLatch latch;
                    if (recordContext.partition() == 0) {
                        counter = counterP0;
                        latch = latchPartition0;
                    } else {
                        counter = counterP1;
                        latch = latchPartition1;
                    }
                    //towards end of records in Partition 0 - throw RTE to get failed record to verify meter
                    if (recordContext.partition() == 0 && counter.get() > (quantityP0 - 300)) {
                        if (!failedRecordDone.getAndSet(true)) {
                            throw new RuntimeException("Failed a record to verify failed meter");
                        }
                    }
                    if (counter.get() >= numberToBlockAt.get()) {
                        latch.await();
                    } else {
                        Thread.sleep(5);
                    }
                    counter.incrementAndGet();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        pcMetricsTracker.bindTo(registry);
        // metrics have some data
        await().atMost(Duration.ofSeconds(300)).untilAsserted(() -> {
            assertFalse(registry.getMeters().isEmpty());
            assertEquals(RUNNING.ordinal(),
                    registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_PC_STATUS, "status", RUNNING.name()));
            assertEquals(2, registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_NUMBER_SHARDS));
            assertEquals(2, registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_NUMBER_PARTITIONS));

        });

        // metrics show processing is complete
        await().untilAsserted(() -> {
            log.info("counterP0: {}, counterP1: {}", counterP0.get(), counterP1.get());
            log.info(registry.getMetersAsString());
            assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_NUMBER_PAUSED_PARTITIONS)).isEqualTo(2);
        });

        //Need to give a little bit of time as racing between scraping metrics and asserts below
        Thread.sleep(1000);
        log.info(registry.getMetersAsString());

        int remainingP0 = quantityP0 - counterP0.get();
        int remainingP1 = quantityP1 - counterP1.get();
        int highestProcessedOffsetP0 = counterP0.get() - 1;
        int highestProcessedOffsetP1 = counterP1.get() + p1StartingOffset - 1;
        int highestSeenOffsetP0 = quantityP0 - 1;
        int highestSeenOffsetP1 = quantityP1 + p1StartingOffset - 1;
        //Assert record counts, offset counts specific to Partition 0
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_HIGHEST_COMPLETED_OFFSET, 0))
                .isEqualTo(highestProcessedOffsetP0);
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_HIGHEST_SEEN_OFFSET, 0)).isEqualTo(
                highestSeenOffsetP0);
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET, 0))
                .isEqualTo(highestProcessedOffsetP0);
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_INCOMPLETE_OFFSETS, 0))
                .isEqualTo(remainingP0);
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_LAST_COMMITTED_OFFSET, 0))
                .isEqualTo(highestProcessedOffsetP0 + 1);
        assertThat(registeredCounterValueFor(PCMetricsTracker.METRIC_NAME_PROCESSED_RECORDS,
                "epoch", "0", "topic", topicPartition.topic(), "partition", String.valueOf(0)))
                .isEqualTo(counterP0.get());


        //Assert same as above for Partition 1
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_HIGHEST_COMPLETED_OFFSET, 1))
                .isEqualTo(highestProcessedOffsetP1);
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_HIGHEST_SEEN_OFFSET, 1)).isEqualTo(
                highestSeenOffsetP1);
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET, 1))
                .isEqualTo(highestProcessedOffsetP1);
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_INCOMPLETE_OFFSETS, 1))
                .isEqualTo(remainingP1);
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_LAST_COMMITTED_OFFSET, 1))
                .isEqualTo(highestProcessedOffsetP1 + 1);
        assertThat(registeredCounterValueFor(PCMetricsTracker.METRIC_NAME_PROCESSED_RECORDS,
                "epoch", "0", "topic", topicPartition.topic(), "partition", String.valueOf(1)))
                .isEqualTo(counterP1.get());

        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_SHARD_SIZE))
                .isEqualTo(remainingP0 + remainingP1);
        // non partition specific metrics
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_TOTAL_INCOMPLETE_OFFSETS))
                .isEqualTo(remainingP0 + remainingP1);

        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_INFLIGHT_RECORDS))
                .isGreaterThan(0); // I think it is CPU number bound as it defaults to some multiplier of available CPUs - so can't assume number based on my machine...

        assertThat(registeredDistributionSummaryFor(PCMetricsTracker.METRIC_NAME_METADATA_SPACE_USED))
                .isGreaterThan(0);

        assertThat(registeredTimerFor(PCMetricsTracker.METRIC_NAME_OFFSETS_ENCODING_TIME))
                .isGreaterThan(0);

        assertThat(registeredCounterValueFor(PCMetricsTracker.METRIC_NAME_OFFSETS_ENCODING_USAGE))
                .isGreaterThan(0);

        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_NUMBER_PARTITIONS))
                .isEqualTo(2);
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_NUMBER_PAUSED_PARTITIONS))
                .isEqualTo(2);

        assertThat(registeredDistributionSummaryFor(PCMetricsTracker.METRIC_NAME_PAYLOAD_RATIO_USED))
                .isGreaterThan(-1.0); // cant really check for actual value as it may be either 0 or >0 depending on timing of commit / encoding execution.

        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_NUMBER_SHARDS))
                .isEqualTo(2);
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_PC_STATUS, "status", RUNNING.name()))
                .isEqualTo(1);

        // it would be remaining - inFlight, but because inFlight number depends on load factor which in turn depends on CPU core number - adding allowable offset.
        assertThat(registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_WAITING_RECORDS))
                .isCloseTo((remainingP0 + remainingP1), Offset.offset(100.0));

        assertThat(registeredTimerFor(PCMetricsTracker.METRIC_NAME_USER_FUNCTION_PROCESSING_TIME)).isGreaterThan(0);

        numberToBlockAt.set(5000);
        latchPartition0.countDown();
        latchPartition1.countDown();
        await().untilAsserted(() -> {
            assertThat(counterP0.get()).isEqualTo(quantityP0);
        });

        await().atMost(Duration.ofSeconds(120)).until(() -> counterP0.get() == quantityP0 &&
                registeredGaugeValueFor(PCMetricsTracker.METRIC_NAME_WAITING_RECORDS) == 0
        );

        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
            log.info(registry.getMetersAsString());
            assertThat(registeredCounterValueFor(PCMetricsTracker.METRIC_NAME_FAILED_RECORDS, "topic", topicPartition.topic(), "partition", String.valueOf(0)))
                    .isEqualTo(1);
        });
    }

    private double registeredGaugeValueFor(String name, String... filterTags) {
        return registry.get(name).tags(filterTags).gauge().value();
    }

    private double registeredGaugeValueFor(String name, int partition) {
        String[] filterTags = new String[]{"topic", topicPartition.topic(), "partition", String.valueOf(partition)};
        return registeredGaugeValueFor(name, filterTags);
    }

    private double registeredCounterValueFor(String name, String... filterTags) {
        return Optional.ofNullable(registry.find(name).tags(filterTags).counter())
                .map(counter -> counter.count()).orElse(-1.0);
    }

    private double registeredTimerFor(String metricName, String... tags) {
        return Optional.ofNullable(registry.find(metricName).tags(tags).timer())
                .map(timer -> timer.mean(TimeUnit.MILLISECONDS)).orElse(-1.0);
    }

    private double registeredDistributionSummaryFor(String metricName, String... tags) {
        return Optional.ofNullable(registry.find(metricName).tags(tags).summary())
                .map(DistributionSummary::mean).orElse(-1.0);
    }
}

