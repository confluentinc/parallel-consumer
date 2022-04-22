package io.confluent.parallelconsumer.sharedstate;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


import lombok.Value;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

@Value
public class PartitionEventMessage {
    PartitionEventType type;
    Collection<TopicPartition> partitions;

    public enum PartitionEventType {
        ASSIGNED, REVOKED
    }
}
