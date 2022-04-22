package io.confluent.parallelconsumer.sharedstate;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


import io.confluent.parallelconsumer.controller.WorkContainer;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

/**
 * An inbound message to the controller.
 * <p>
 * Currently, an Either type class, representing either newly polled records to ingest, or a work result.
 */
@AllArgsConstructor
@RequiredArgsConstructor(access = PRIVATE)
@Getter
@EqualsAndHashCode
public class ControllerEventMessage<K, V> {
    WorkContainer<K, V> workContainer = null;
    EpochAndRecordsMap<K, V> consumerRecords = null;
    PartitionEventMessage partitionEventMessage = null;
    CommitData commitData = null;

    public boolean isWorkResult() {
        return workContainer != null;
    }

    public boolean isNewConsumerRecords() {
        return consumerRecords != null;
    }

    public boolean isPartitionEvent() {
        return partitionEventMessage != null;
    }

    public boolean isCommitSuccessEvent() {
        return commitData != null;
    }

    public ControllerEventMessage(EpochAndRecordsMap<K, V> polledRecords) {
        this.consumerRecords = polledRecords;
    }

    public ControllerEventMessage(CommitData event) {
        this.commitData = event;
    }

    public static <K, V> ControllerEventMessage<K, V> of(EpochAndRecordsMap<K, V> polledRecords) {
        return new ControllerEventMessage<>(null, polledRecords, null, null);
    }

    public static <K, V> ControllerEventMessage<K, V> of(WorkContainer<K, V> work) {
        return new ControllerEventMessage<>(work, null, null, null);
    }

    public static <K, V> ControllerEventMessage<K, V> of(PartitionEventMessage event) {
        return new ControllerEventMessage<>(null, null, event, null);
    }

}
