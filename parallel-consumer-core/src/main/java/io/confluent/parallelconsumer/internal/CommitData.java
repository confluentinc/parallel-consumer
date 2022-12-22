package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Antony Stubbs
 */
@Value
@RequiredArgsConstructor
public class CommitData {

    public static final OffsetAndMetadata DEFAULT_OFFSET_AND_METADATA = new OffsetAndMetadata(0);

    @Delegate
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;

    public CommitData() {
        this(UniMaps.of());
    }

    public CommitData(List<TopicPartition> partitions) {
        this(partitions.stream()
                .collect(Collectors.toMap(tp
                        -> tp, tp -> DEFAULT_OFFSET_AND_METADATA)));
    }

    public CommitData(TopicPartition topicPartitionOf) {
        this(UniLists.of(topicPartitionOf));
    }
}
