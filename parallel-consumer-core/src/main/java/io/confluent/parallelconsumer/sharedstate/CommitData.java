package io.confluent.parallelconsumer.sharedstate;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Value;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

@Value
public class CommitData {
    @Delegate
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
}
