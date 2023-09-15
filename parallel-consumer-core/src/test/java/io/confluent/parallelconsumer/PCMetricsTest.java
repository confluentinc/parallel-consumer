package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.data.Offset;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.internal.State.PAUSED;
import static io.confluent.parallelconsumer.internal.State.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Slf4j
class PCMetricsTest extends ParallelEoSStreamProcessorTestBase {
    private SimpleMeterRegistry registry;
    private final List<Tag> commonTags = UniLists.of(Tag.of("tag1", "pc1"));

//    @Test
//    @SneakyThrows
//    void metricsRegisterBinding() {
//        final int quantityP0 = 1000;
//        final int quantityP1 = 500;
//        AtomicInteger numberToBlockAt = new AtomicInteger(200);
//        final int p1StartingOffset = quantityP0;
//
//        ktu.send(consumerSpy, ktu.generateRecords(0, quantityP0));
//        ktu.send(consumerSpy, ktu.generateRecords(1, quantityP1));
//        CountDownLatch latchPartition0 = new CountDownLatch(1);
//        CountDownLatch latchPartition1 = new CountDownLatch(1);
//        AtomicInteger counterP0 = new AtomicInteger();
//        AtomicInteger counterP1 = new AtomicInteger();
//        AtomicBoolean failedRecordDone = new AtomicBoolean(false);
//        parallelConsumer.poll(recordContexts -> {
//            recordContexts.forEach(recordContext -> {
//                log.trace("Processing: {}", recordContext);
//                try {
//                    AtomicInteger counter;
//                    CountDownLatch latch;
//                    if (recordContext.partition() == 0) {
//                        counter = counterP0;
//                        latch = latchPartition0;
//                    } else {
//                        counter = counterP1;
//                        latch = latchPartition1;
//                    }
//                    //towards end of records in Partition 0 - throw RTE to get failed record to verify meter
//                    if (recordContext.partition() == 0 && counter.get() > (quantityP0 - 300)) {
//                        if (!failedRecordDone.getAndSet(true)) {
//                            throw new RuntimeException("Failed a record to verify failed meter");
//                        }
//                    }
//                    if (counter.get() >= numberToBlockAt.get()) {
//                        latch.await();
//                    } else {
//                        Thread.sleep(5);
//                    }
//                    counter.incrementAndGet();
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            });
//        });
//
//        log.info(registry.getMetersAsString());
//        // metrics have some data
//        await().atMost(Duration.ofSeconds(300)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
//            assertFalse(registry.getMeters().isEmpty());
//            assertEquals(RUNNING.getValue(), registeredGaugeValueFor(PCMetricsDef.PC_STATUS));
//            assertEquals(2, registeredGaugeValueFor(PCMetricsDef.NUMBER_OF_SHARDS));
//            assertEquals(2, registeredGaugeValueFor(PCMetricsDef.NUMBER_OF_PARTITIONS));
//        });
//
//        // metrics show processing is complete
//        await().untilAsserted(() -> {
//            log.info("counterP0: {}, counterP1: {}", counterP0.get(), counterP1.get());
//            log.info(registry.getMetersAsString());
//            assertThat(registeredGaugeValueFor(PCMetricsDef.NUM_PAUSED_PARTITIONS)).isEqualTo(2);
//        });
//
//        //Need to give a little bit of time as racing between scraping metrics and asserts below
//        Thread.sleep(1000);
//        log.info(registry.getMetersAsString());
//
//        int remainingP0 = quantityP0 - counterP0.get();
//        int remainingP1 = quantityP1 - counterP1.get();
//        int highestProcessedOffsetP0 = counterP0.get() - 1;
//        int highestProcessedOffsetP1 = counterP1.get() + p1StartingOffset - 1;
//        int highestSeenOffsetP0 = quantityP0 - 1;
//        int highestSeenOffsetP1 = quantityP1 + p1StartingOffset - 1;
//        //Assert record counts, offset counts specific to Partition 0
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_COMPLETED_OFFSET, 0))
//                .isEqualTo(highestProcessedOffsetP0);
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_SEEN_OFFSET, 0)).isEqualTo(
//                highestSeenOffsetP0);
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET, 0))
//                .isEqualTo(highestProcessedOffsetP0);
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_INCOMPLETE_OFFSETS, 0))
//                .isEqualTo(remainingP0);
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_LAST_COMMITTED_OFFSET, 0))
//                .isEqualTo(highestProcessedOffsetP0 + 1);
//        assertThat(registeredCounterValueFor(PCMetricsDef.PROCESSED_RECORDS,
//                "topic", topicPartition.topic(), "partition", String.valueOf(0)))
//                .isEqualTo(counterP0.get());
//
//
//        //Assert same as above for Partition 1
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_COMPLETED_OFFSET, 1))
//                .isEqualTo(highestProcessedOffsetP1);
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_SEEN_OFFSET, 1)).isEqualTo(
//                highestSeenOffsetP1);
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET, 1))
//                .isEqualTo(highestProcessedOffsetP1);
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_INCOMPLETE_OFFSETS, 1))
//                .isEqualTo(remainingP1);
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_LAST_COMMITTED_OFFSET, 1))
//                .isEqualTo(highestProcessedOffsetP1 + 1);
//        assertThat(registeredCounterValueFor(PCMetricsDef.PROCESSED_RECORDS, "topic", topicPartition.topic(), "partition", String.valueOf(1)))
//                .isEqualTo(counterP1.get());
//
//        assertThat(registeredGaugeValueFor(PCMetricsDef.SHARDS_SIZE))
//                .isEqualTo(remainingP0 + remainingP1);
//        // non partition specific metrics
//        assertThat(registeredGaugeValueFor(PCMetricsDef.INCOMPLETE_OFFSETS_TOTAL))
//                .isEqualTo(remainingP0 + remainingP1);
//
//        assertThat(registeredGaugeValueFor(PCMetricsDef.INFLIGHT_RECORDS))
//                .isGreaterThan(0); // I think it is CPU number bound as it defaults to some multiplier of available CPUs - so can't assume number based on my machine...
//
//        assertThat(registeredDistributionSummaryFor(PCMetricsDef.METADATA_SPACE_USED))
//                .isGreaterThan(0);
//
//        assertThat(registeredTimerFor(PCMetricsDef.OFFSETS_ENCODING_TIME))
//                .isGreaterThan(0);
//
//        assertThat(registeredCounterValueFor(PCMetricsDef.OFFSETS_ENCODING_USAGE))
//                .isGreaterThan(0);
//
//        assertThat(registeredGaugeValueFor(PCMetricsDef.NUMBER_OF_PARTITIONS))
//                .isEqualTo(2);
//        assertThat(registeredGaugeValueFor(PCMetricsDef.NUM_PAUSED_PARTITIONS))
//                .isEqualTo(2);
//
//        assertThat(registeredDistributionSummaryFor(PCMetricsDef.PAYLOAD_RATIO_USED))
//                .isGreaterThan(-1.0); // cant really check for actual value as it may be either 0 or >0 depending on timing of commit / encoding execution.
//
//        assertThat(registeredGaugeValueFor(PCMetricsDef.NUMBER_OF_SHARDS))
//                .isEqualTo(2);
//        assertThat(registeredGaugeValueFor(PCMetricsDef.PC_STATUS))
//                .isEqualTo(RUNNING.getValue());
//
//        // it would be remaining - inFlight, but because inFlight number depends on load factor which in turn depends on CPU core number - adding allowable offset.
//        assertThat(registeredGaugeValueFor(PCMetricsDef.WAITING_RECORDS))
//                .isCloseTo((remainingP0 + remainingP1), Offset.offset(100.0));
//
//        assertThat(registeredTimerFor(PCMetricsDef.USER_FUNCTION_PROCESSING_TIME)).isGreaterThan(0);
//
//        numberToBlockAt.set(5000);
//        latchPartition0.countDown();
//        latchPartition1.countDown();
//        await().untilAsserted(() -> {
//            assertThat(counterP0.get()).isEqualTo(quantityP0);
//        });
//
//        await().atMost(Duration.ofSeconds(120)).until(() -> counterP0.get() == quantityP0 &&
//                registeredGaugeValueFor(PCMetricsDef.WAITING_RECORDS) == 0
//        );
//
//        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
//            log.info(registry.getMetersAsString());
//            assertThat(registeredCounterValueFor(PCMetricsDef.FAILED_RECORDS, "topic", topicPartition.topic(), "partition", String.valueOf(0)))
//                    .isEqualTo(1);
//        });
//    }


    @Test
    @SneakyThrows
    void pcStatusMetricUpdatesOnChange() {
        final int quantity = 1000;

        ktu.send(consumerSpy, ktu.generateRecords(0, quantity));

        parallelConsumer.poll(recordContexts -> {
            recordContexts.forEach(recordContext -> {
                log.trace("Processing: {}", recordContext);
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        log.info(registry.getMetersAsString());
        // metrics have some data
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertFalse(registry.getMeters().isEmpty());
            assertEquals(RUNNING.getValue(),
                    registeredGaugeValueFor(PCMetricsDef.PC_STATUS));
        });

        parallelConsumer.pauseIfRunning();
        ktu.send(consumerSpy, ktu.generateRecords(0, 100));
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertEquals(PAUSED.getValue(),
                    registeredGaugeValueFor(PCMetricsDef.PC_STATUS));
        });
        parallelConsumer.resumeIfPaused();
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertEquals(RUNNING.getValue(),
                    registeredGaugeValueFor(PCMetricsDef.PC_STATUS));
        });
    }


    private double registeredGaugeValueFor(PCMetricsDef metricsDef, String... filterTags) {
        return Optional.ofNullable(registry.find(metricsDef.getName()).tags(filterTags).gauge()).map(Gauge::value).orElse(-1.0);
    }

    private double registeredGaugeValueFor(PCMetricsDef metricsDef, int partition) {
        String[] filterTags = new String[]{"topic", topicPartition.topic(), "partition", String.valueOf(partition)};
        return registeredGaugeValueFor(metricsDef, filterTags);
    }

    private double registeredCounterValueFor(PCMetricsDef metricsDef, String... filterTags) {
        return Optional.ofNullable(registry.find(metricsDef.getName()).tags(filterTags).counter())
                .map(Counter::count).orElse(-1.0);
    }

    private double registeredTimerFor(PCMetricsDef metricsDef, String... tags) {
        return Optional.ofNullable(registry.find(metricsDef.getName()).tags(tags).timer())
                .map(timer -> timer.mean(TimeUnit.MILLISECONDS)).orElse(-1.0);
    }

    private double registeredDistributionSummaryFor(PCMetricsDef metricsDef, String... tags) {
        return Optional.ofNullable(registry.find(metricsDef.getName()).tags(tags).summary())
                .map(DistributionSummary::mean).orElse(-1.0);
    }

    @Override
    protected ParallelConsumerOptions<Object, Object> getOptions() {
        registry = new SimpleMeterRegistry(new SimpleConfig() {
            @Override
            public String get(final String key) {
                return null;
            }

            @Override
            public @NotNull Duration step() {
                return Duration.ofSeconds(10);
            }
        }, Clock.SYSTEM);
        ParallelConsumerOptions<Object, Object> options = getDefaultOptions()
                .meterRegistry(registry)
                .metricsTags(commonTags)
                .build();

        return options;
    }
}

