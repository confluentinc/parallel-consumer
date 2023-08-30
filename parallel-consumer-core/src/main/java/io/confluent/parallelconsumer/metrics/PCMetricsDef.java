package io.confluent.parallelconsumer.metrics;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.internal.State;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.Getter;

import java.util.Arrays;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.metrics.PCMetricsDef.MeterType.*;

/**
 * This enum defines the metrics that are collected by the PC.
 */
public enum PCMetricsDef {

    USER_FUNCTION_PROCESSING_TIME("user.function.processing.time", "User function processing time", PCMetricsSubsystem.PROCESSOR, TIMER),
    DYNAMIC_EXTRA_LOAD_FACTOR("dynamic.load.factor", "Dynamic load factor - load of processing buffers", PCMetricsSubsystem.PROCESSOR, GAUGE),

    INFLIGHT_RECORDS("inflight.records", "Total number of records currently being processed or waiting for retry", PCMetricsSubsystem.WORK_MANAGER, GAUGE),
    WAITING_RECORDS("waiting.records", "Total number of records waiting to be selected for processing", PCMetricsSubsystem.WORK_MANAGER, GAUGE),
    PROCESSED_RECORDS("processed.records", "Total number of records successfully processed", PCMetricsSubsystem.WORK_MANAGER, COUNTER, topicPartitionTags()),
    FAILED_RECORDS("failed.records", "Total number of records failed to be processed", PCMetricsSubsystem.WORK_MANAGER, COUNTER, topicPartitionTags()),
    SLOW_RECORDS("slow.records", "Total number of records that spent more than the configured time threshold in the waiting queue. This setting defaults to 10 seconds", PCMetricsSubsystem.WORK_MANAGER, COUNTER, topicPartitionTags()),

    PC_POLLER_STATUS("poller.status", "PC Broker Poller Status, reported as number with following mapping - " + getStateToValueListing(), PCMetricsSubsystem.BROKER_POLLER, GAUGE),
    PC_STATUS("status", "PC Status, reported as number with following mapping - " + getStateToValueListing(), PCMetricsSubsystem.PROCESSOR, GAUGE),
    NUM_PAUSED_PARTITIONS("partitions.paused", "Number of paused partitions", PCMetricsSubsystem.BROKER_POLLER, GAUGE),


    NUMBER_OF_SHARDS("shards", "Number of shards", PCMetricsSubsystem.SHARD_MANAGER, GAUGE),

    INCOMPLETE_OFFSETS_TOTAL("incomplete.offsets.total", "Total number of incomplete offsets", PCMetricsSubsystem.SHARD_MANAGER, GAUGE),
    SHARDS_SIZE("shards.size", "Number of records queued for processing across all shards", PCMetricsSubsystem.SHARD_MANAGER, GAUGE),


    //TODO: Not implemented yet - add to Metrics.adoc when implemented
    // AVERAGE_USER_PROCESSING_TIME("avg.processing.time", "Average user function processing time", PCMetricsSubsystem.SHARD_MANAGER),

    //TODO: Not implemented yet - add to Metrics.adoc when implemented
    // AVERAGE_WAITING_TIME("avg.waiting.time", "Average waiting time in the processing queue", PCMetricsSubsystem.SHARD_MANAGER),

    NUMBER_OF_PARTITIONS("partitions.number", "Number of partitions", PCMetricsSubsystem.PARTITION_MANAGER, GAUGE),
    PARTITION_INCOMPLETE_OFFSETS("partition.incomplete.offsets", "Number of incomplete offsets in the partition", PCMetricsSubsystem.PARTITION_MANAGER, GAUGE, topicPartitionTags()),
    PARTITION_HIGHEST_COMPLETED_OFFSET("partition.highest.completed.offset", "Highest completed offset in the partition", PCMetricsSubsystem.PARTITION_MANAGER, GAUGE, topicPartitionTags()),
    PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET("partition.highest.sequential.succeeded.offset", "Highest sequential succeeded offset in the partition", PCMetricsSubsystem.PARTITION_MANAGER, GAUGE, topicPartitionTags()),
    PARTITION_HIGHEST_SEEN_OFFSET("partition.highest.seen.offset", "Highest seen / consumed offset in the partition", PCMetricsSubsystem.PARTITION_MANAGER, GAUGE, topicPartitionTags()),
    PARTITION_LAST_COMMITTED_OFFSET("partition.latest.committed.offset", "Latest committed offset in the partition", PCMetricsSubsystem.PARTITION_MANAGER, GAUGE, topicPartitionTags()),
    PARTITION_ASSIGNMENT_EPOCH("partition.assignment.epoch", "Epoch of partition assignment", PCMetricsSubsystem.PARTITION_MANAGER, GAUGE, topicPartitionTags()),


    OFFSETS_ENCODING_TIME("offsets.encoding.time", "Time spend encoding offsets", PCMetricsSubsystem.OFFSET_ENCODER, TIMER),
    OFFSETS_ENCODING_USAGE("offsets.encoding.usage", "Offset encoding usage per encoding type", PCMetricsSubsystem.OFFSET_ENCODER, COUNTER, tag("codec", "BitSet|BitSetCompressed|BitSetV2Compressed|RunLength")),
    METADATA_SPACE_USED("metadata.space.used", "Ratio between offset metadata payload size and available space", PCMetricsSubsystem.OFFSET_ENCODER, DISTRIBUTION_SUMMARY),
    PAYLOAD_RATIO_USED("payload.ratio.used", "Ratio between offset metadata payload size and offsets encoded", PCMetricsSubsystem.OFFSET_ENCODER, DISTRIBUTION_SUMMARY);

    public static final String PC_INSTANCE_TAG = "pcinstance";

    private static String getStateToValueListing() {
        return Arrays.stream(State.values()).map(state -> state.getValue() + ":" + state).collect(Collectors.joining(", "));
    }

    private static ParallelConsumer.Tuple<String, String>[] topicPartitionTags() {
        return new ParallelConsumer.Tuple[]{tag("topic", "topicName"), tag("partition", "partitionNumber")};
    }

    private static final String SUBSYSTEM_TAG_KEY = "subsystem";

    private static final String METER_PREFIX = "pc.";
    public static final String USER_FUNCTION_EXECUTOR_PREFIX = METER_PREFIX+"user.function.";

    @Getter
    private final String name;
    @Getter
    private final String description;
    private final ParallelConsumer.Tuple<String, String>[] tags;
    private final MeterType type;
    @Getter
    private Tag subsystem;

    /**
     * @param name:        the name of the metric
     * @param description: A quick summary of the metric
     * @param subsystem:   PC subsystem tag for meter grouping
     * @param type:        Meter type - Counter, Timer etc - only for meter definition markdown generation
     * @param tags:        Metrics tags - keys of tags used for meters - only for meter definition markdown generation
     */
    PCMetricsDef(String name, String description, PCMetricsSubsystem subsystem, MeterType type, ParallelConsumer.Tuple<String, String>... tags) {
        this.name = METER_PREFIX + name;
        this.description = description;
        this.tags = tags;
        if (subsystem != null) {
            this.subsystem = Tag.of(SUBSYSTEM_TAG_KEY, subsystem.subsystemTag);
        }
        this.type = type;
    }

    public Tags getSubsystemAsTagsOrEmpty() {
        if (this.subsystem == null) {
            return Tags.empty();
        } else {
            return Tags.of(subsystem);
        }
    }

    //Formatting methods for markdown generation
    public static void main(String[] args) {
        System.out.println(toMarkdown());
    }

    private static String toCamelCase(String s) {
        return Arrays.stream(s.replace("_", " ").split(" ")).map(string -> string.substring(0, 1).toUpperCase() + string.substring(1).toLowerCase()).collect(Collectors.joining(" "));
    }

    private static String formatMetricDef(PCMetricsDef metricsDef) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("**%s**%n%n", toCamelCase(metricsDef.name())));
        sb.append(String.format("%s `%s%s`%n%n", toCamelCase(metricsDef.type.name()), metricsDef.name, formatTagsAndSubsystem(metricsDef)));
        sb.append(metricsDef.description);
        sb.append("\n\n");
        return sb.toString();
    }

    private static String formatTagsAndSubsystem(PCMetricsDef metricsDef) {
        String res = "";
        if (metricsDef.subsystem != null) {
            res += String.format("%s=%s", metricsDef.subsystem.getKey(), metricsDef.subsystem.getValue());
        }
        if (metricsDef.tags != null && metricsDef.tags.length > 0) {
            if (res.length() > 0) {
                res += ", ";
            }
            res += joinTagsForRendering(metricsDef.tags);
        }
        if (res.length() > 0) {
            res = "{" + res + "}";
        }
        return res;
    }

    /**
     * Outputs a markdown representation of the above enums
     */
    private static String toMarkdown() {
        StringBuilder sb = new StringBuilder();
        Arrays.stream(PCMetricsSubsystem.values()).forEach(sub -> {
                    sb.append(formatSubsystem(sub));
                    Arrays.stream(PCMetricsDef.values())
                            .filter(def -> def.subsystem != null && def.subsystem.getValue().equals(sub.subsystemTag))
                            .forEach(v -> sb.append(formatMetricDef(v)));
                }
        );
        return sb.toString();
    }

    private static String formatSubsystem(PCMetricsSubsystem sub) {
        return String.format("==== %s%n%n", toCamelCase(sub.name()));
    }

    private static String joinTagsForRendering(ParallelConsumer.Tuple<String, String>[] tags) {
        if (tags == null || tags.length == 0) {
            return "";
        }
        return Arrays.stream(tags).map(tag -> tag.getLeft() + "=\"" + tag.getRight() + "\"")
                .collect(Collectors.joining(", "));
    }

    static ParallelConsumer.Tuple<String, String> tag(String key, String valueToken) {
        return ParallelConsumer.Tuple.pairOf(key, valueToken);
    }

    /**
     * Metrics are divided into subsystems for better representation and fine-grained filtering.
     */
    public enum PCMetricsSubsystem {
        PARTITION_MANAGER("partitions"),
        PROCESSOR("processor"),
        SHARD_MANAGER("shardmanager"),
        WORK_MANAGER("workmanager"),
        BROKER_POLLER("poller"),
        OFFSET_ENCODER("offsetencoder");

        private final String subsystemTag;

        PCMetricsSubsystem(String subsystemTag) {
            this.subsystemTag = subsystemTag;
        }
    }

    public enum MeterType {
        COUNTER,
        TIMER,
        GAUGE,
        DISTRIBUTION_SUMMARY
    }
}
