package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import com.google.common.eventbus.Subscribe;
import io.confluent.parallelconsumer.state.ShardKey;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

/**
 * Metrics binder for Parallel Consumer
 *
 * @author Nacho Munoz
 */
public class PCMetricsTracker implements MeterBinder, AutoCloseable {
    public static final long REFRESH_INTERVAL_SECONDS = 30;
    public static final long INITIAL_DELAY_SECONDS = 5;
    public static final String PC_METRIC_NAME_PREFIX = "pc";
    public static final String METRIC_NAME_PC_STATUS = PC_METRIC_NAME_PREFIX + ".status";
    public static final String METRIC_NAME_NUMBER_PARTITIONS = PC_METRIC_NAME_PREFIX + ".partitions";
    public static final String METRIC_NAME_NUMBER_PAUSED_PARTITIONS = PC_METRIC_NAME_PREFIX + ".paused.partitions";
    public static final String METRIC_NAME_NUMBER_SHARDS = PC_METRIC_NAME_PREFIX + ".shards";
    public static final String METRIC_NAME_SHARD_SIZE = PC_METRIC_NAME_PREFIX + ".shard.size";
    public static final String METRIC_NAME_AVERAGE_USER_PROCESSING_TIME = PC_METRIC_NAME_PREFIX + ".avg.processing.time";
    public static final String METRIC_NAME_AVERAGE_WAITING_TIME = PC_METRIC_NAME_PREFIX + ".avg.waiting.time";
    public static final String METRIC_NAME_TOTAL_INCOMPLETE_OFFSETS = PC_METRIC_NAME_PREFIX + ".incomplete.offsets.total";
    public static final String METRIC_NAME_INCOMPLETE_OFFSETS = PC_METRIC_NAME_PREFIX + ".incomplete.offsets.partition";
    public static final String METRIC_NAME_HIGHEST_COMPLETED_OFFSET = PC_METRIC_NAME_PREFIX + ".highest.complete.offset.partition";
    public static final String METRIC_NAME_HIGHEST_SEEN_OFFSET = PC_METRIC_NAME_PREFIX + ".highest.seen.offset";
    public static final String METRIC_NAME_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET = PC_METRIC_NAME_PREFIX + ".highest.sequential.succeeded.offset.partition";
    public static final String METRIC_NAME_LAST_COMMITTED_OFFSET = PC_METRIC_NAME_PREFIX + ".latest.commited.offset.partition";
    public static final  String METRIC_NAME_PROCESSED_RECORDS =  PC_METRIC_NAME_PREFIX + ".processed.records";
    public static final String METRIC_NAME_FAILED_RECORDS =  PC_METRIC_NAME_PREFIX + ".failed.records";
    public static final String METRIC_NAME_OFFSETS_ENCODING_TIME =  PC_METRIC_NAME_PREFIX + ".offsets.encoding.time";
    public static final String METRIC_NAME_OFFSETS_ENCODING_USAGE =  PC_METRIC_NAME_PREFIX + ".offsets.encoding.usage";
    public static final String METRIC_NAME_USER_FUNCTION_PROCESSING_TIME =  PC_METRIC_NAME_PREFIX + ".user.function.processing.time";

    private static final String METRIC_CATEGORY = "subsystem";

    private static final String METRIC_PARTITION_MANAGER_CATEGORY = "partitions";

    private static final String METRIC_SHARD_MANAGER_CATEGORY = "shardmanager";

    private static final String METRIC_TOPIC = "topic";

    private static final String METRIC_PARTITION = "partition";

    private static final String METRIC_SHARD_KEY = "shard";

    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory("micrometer-pc-metrics"));

    private final Set<Meter.Id> registeredMeterIds = ConcurrentHashMap.newKeySet();

    private final Supplier<PCMetrics> pcMetricsSupplier;

    private MeterRegistry meterRegistry;

    private volatile PCMetrics pcMetrics;

    private final Iterable<Tag> commonTags;

    public PCMetricsTracker(Supplier<PCMetrics> pcMetricsSupplier) {
        this(pcMetricsSupplier, Collections.emptyList());
    }

    public PCMetricsTracker(Supplier<PCMetrics> pcMetricsSupplier, Iterable<Tag> tags) {
        this.pcMetricsSupplier = pcMetricsSupplier;
        this.pcMetrics = PCMetrics.builder()
                .partitionMetrics(Collections.emptyMap())
                .shardMetrics(Collections.emptyMap())
                .build();

        this.commonTags = tags;
    }

    @Subscribe
    public void eventHandler(final MetricsEvent event){
        switch (event.getType()){
            case COUNTER -> Optional.ofNullable(
                    meterRegistry.find(event.getName()).tags(event.getTags()).counter()).orElseGet(()->
                    bindAndGetCounter(event.getName(), event.getDescription(), event.getTags())).increment();
             case TIMER -> Optional.ofNullable(
                            meterRegistry.find(event.getName()).tags(event.getTags()).timer()).orElseGet(()->
                             bindAndGetTimer(event.getName(), event.getDescription(), event.getTags()))
                    .record(event.getTimerValue());
             case GAUGE -> throw new UnsupportedOperationException("Not Implemented yet");
        }
    }

    private Timer bindAndGetTimer(String name, String desc, Iterable<Tag> tags) {
        return Timer.builder(name)
                .description(desc)
                .publishPercentileHistogram(true)
                .publishPercentiles(0.5,0.95,0.99,0.999)
                .tags(tags)
                .register(this.meterRegistry);
    }

    private Counter bindAndGetCounter(String name, String desc, Iterable<Tag> tags) {
        return Counter.builder(name)
                .description(desc)
                .tags(tags)
                .register(this.meterRegistry);
    }


    private void bindGauge(String name, String desc, ToDoubleFunction<PCMetricsTracker> fn, Iterable<Tag> tags) {
        var gauge = Gauge.builder(name, this, fn)
                .description(desc)
                .tags(commonTags)
                .tags(tags)
                .register(this.meterRegistry);

        registeredMeterIds.add(gauge.getId());
    }

    private void removeMetersBySubsystem(String subsystemTag) {
        this.registeredMeterIds.stream()
                .filter(id -> !this.meterRegistry
                        .find(id.getName())
                        .tags(METRIC_CATEGORY, subsystemTag)
                        .meters().isEmpty())
                .map(this.meterRegistry::remove)
                .forEach(m -> this.registeredMeterIds.remove(m.getId()));
    }

    private void bindTimeGauge(String name, String desc, ToDoubleFunction<PCMetricsTracker> fn, Iterable<Tag> tags) {
        var gauge = TimeGauge.builder(name, this, TimeUnit.MILLISECONDS, fn)
                .description(desc)
                .tags(commonTags)
                .tags(tags)
                .register(this.meterRegistry);

        registeredMeterIds.add(gauge.getId());
    }

    private void defineMetersPartitionManager(TopicPartition tp, PCMetrics.PCPartitionMetrics pm) {
        final var partitionTags = UniLists.of(Tag.of(METRIC_CATEGORY, METRIC_PARTITION_MANAGER_CATEGORY),
                Tag.of(METRIC_TOPIC, tp.topic()),
                Tag.of(METRIC_PARTITION, String.valueOf(tp.partition())));

        bindGauge(METRIC_NAME_INCOMPLETE_OFFSETS, "Number of incomplete offsets in the partition",
                tracker -> tracker.pcMetrics.getPartitionMetrics().get(tp).getNumberOfIncompletes(),
                partitionTags);

        bindGauge(METRIC_NAME_HIGHEST_COMPLETED_OFFSET, "Highest completed offset in the partition",
                tracker -> tracker.pcMetrics.getPartitionMetrics().get(tp).getHighestCompletedOffset(),
                partitionTags);

        bindGauge(METRIC_NAME_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET, "Highest sequential succeeded offset in the partition",
                tracker -> tracker.pcMetrics.getPartitionMetrics().get(tp).getHighestSequentialSucceededOffset(),
                partitionTags);

        bindGauge(METRIC_NAME_HIGHEST_SEEN_OFFSET, "Highest consumed offset in the partition",
                tracker -> tracker.pcMetrics.getPartitionMetrics().get(tp).getHighestSeenOffset(),
                partitionTags);

        bindGauge(METRIC_NAME_LAST_COMMITTED_OFFSET, "Latest commited offset in the partition",
                tracker -> tracker.pcMetrics.getPartitionMetrics().get(tp).getLastCommittedOffset(),
                partitionTags);
    }

    private void defineMetersShardManager(ShardKey key, PCMetrics.ShardMetrics sm) {
        final var shardTags = UniLists.of(
                Tag.of(METRIC_CATEGORY, METRIC_SHARD_MANAGER_CATEGORY),
                Tag.of(METRIC_SHARD_KEY, key.toString()));

        bindGauge(METRIC_NAME_SHARD_SIZE, "Shard size",
                tracker -> tracker.pcMetrics.getShardMetrics().get(key).getShardSize(),
                shardTags);

        bindTimeGauge(METRIC_NAME_AVERAGE_USER_PROCESSING_TIME, "Average user processing time",
                tracker -> tracker.pcMetrics.getShardMetrics().get(key).getAverageUserProcessingTime(),
                shardTags);

        bindTimeGauge(METRIC_NAME_AVERAGE_WAITING_TIME, "Average waiting time in the processing queue",
                tracker -> tracker.pcMetrics.getShardMetrics().get(key).getAverageTimeSpentInQueue(),
                shardTags);
    }

    private void defineMeters() {
        final int partitionsMetricsCount = pcMetrics.getPartitionMetrics().size();
        final int shardsMetricsCount = pcMetrics.getShardMetrics().size();

        pcMetrics = this.pcMetricsSupplier.get();

        if (registeredMeterIds.isEmpty()) {
            bindGauge(METRIC_NAME_NUMBER_SHARDS, "Number of shards",
                    tracker -> tracker.pcMetrics.getNumberOfShards(),
                    Collections.emptyList());

            bindGauge(METRIC_NAME_TOTAL_INCOMPLETE_OFFSETS, "Total number of incomplete offsets",
                    tracker -> tracker.pcMetrics.getTotalNumberOfIncompletes(),
                    Collections.emptyList());

            bindGauge(METRIC_NAME_NUMBER_PARTITIONS, "Number of partitions",
                    tracker -> tracker.pcMetrics.getNumberOfPartitions(),
                    Collections.emptyList());
        }

        bindGauge(METRIC_NAME_PC_STATUS, "PC Status",
                tracker -> tracker.pcMetrics.getPollerMetrics().getState().ordinal(),
                UniLists.of(Tag.of("status", pcMetrics.getPollerMetrics().getState().name())));

        bindGauge(METRIC_NAME_NUMBER_PAUSED_PARTITIONS, "Number of paused partitions",
                tracker -> Optional.ofNullable(tracker.pcMetrics.getPollerMetrics().getPausedPartitions())
                        .orElseGet(()->Collections.emptyMap()).size(),
                UniLists.of(Tag.of("status", pcMetrics.getPollerMetrics().getState().name())));

        if (partitionsMetricsCount != pcMetrics.getPartitionMetrics().size()) {
            removeMetersBySubsystem(METRIC_PARTITION_MANAGER_CATEGORY);
            pcMetrics.getPartitionMetrics().forEach(this::defineMetersPartitionManager);
        }

        if (shardsMetricsCount != pcMetrics.getShardMetrics().size()) {
            removeMetersBySubsystem(METRIC_SHARD_MANAGER_CATEGORY);
            pcMetrics.getShardMetrics().forEach(this::defineMetersShardManager);
        }
    }

    @Override
    public void bindTo(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        defineMeters();
        this.scheduler.scheduleAtFixedRate(
                this::defineMeters,
                INITIAL_DELAY_SECONDS, REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        this.scheduler.shutdownNow();
        registeredMeterIds.forEach(this.meterRegistry::remove);
    }
}
