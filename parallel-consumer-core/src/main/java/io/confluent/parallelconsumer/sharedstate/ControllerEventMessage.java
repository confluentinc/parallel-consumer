package io.confluent.parallelconsumer.sharedstate;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


import io.confluent.parallelconsumer.controller.WorkContainer;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import static lombok.AccessLevel.PRIVATE;

/**
 * An inbound message to the controller.
 * <p>
 * Currently, an Either type class, representing either newly polled records to ingest, or a work result.
 */
@Value
@RequiredArgsConstructor(access = PRIVATE)
public class ControllerEventMessage<K, V> {
    WorkContainer<K, V> workContainer;
    EpochAndRecordsMap<K, V> consumerRecords;
    PartitionEventMessage partitionEventMessage;

    public boolean isWorkResult() {
        return workContainer != null;
    }

    public boolean isNewConsumerRecords() {
        return consumerRecords != null;
    }

    public boolean isPartitionEvent() {
        return partitionEventMessage != null;
    }

    public static <K, V> ControllerEventMessage<K, V> of(EpochAndRecordsMap<K, V> polledRecords) {
        return new ControllerEventMessage<>(null, polledRecords, null);
    }

    public static <K, V> ControllerEventMessage<K, V> of(WorkContainer<K, V> work) {
        return new ControllerEventMessage<>(work, null, null);
    }

    public static <K, V> ControllerEventMessage<K, V> of(PartitionEventMessage event) {
        return new ControllerEventMessage<>(null, null, event);
    }
}
