package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Map;

/**
 * @author Antony Stubbs
 */
@Value
@RequiredArgsConstructor
public class CommitData {

    @Delegate
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;

    public CommitData() {
        this(UniMaps.of());
    }

}
