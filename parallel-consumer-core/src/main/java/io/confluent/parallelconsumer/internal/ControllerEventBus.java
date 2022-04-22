package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.controller.WorkContainer;
import io.confluent.parallelconsumer.sharedstate.CommitData;
import io.confluent.parallelconsumer.sharedstate.ControllerEventMessage;
import io.confluent.parallelconsumer.sharedstate.PartitionEventMessage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static lombok.AccessLevel.PROTECTED;

@Slf4j
public class ControllerEventBus<K, V> {

    /**
     * Collection of work waiting to be
     */
    @Getter(PROTECTED)
    private final BlockingQueue<ControllerEventMessage<K, V>> workMailBox = new LinkedBlockingQueue<>(); // Thread safe, highly performant, non blocking

    public void sendWorkResultEvent(WorkContainer<K, V> wc) {
        var message = ControllerEventMessage.of(wc);
        addMessage(message);
    }

    private void addMessage(ControllerEventMessage<K, V> message) {
        log.debug("Adding {} to mailbox...", message);
        workMailBox.add(message);
    }

    public void sendConsumerRecordsEvent(EpochAndRecordsMap<K, V> polledRecords) {
        var message = ControllerEventMessage.of(polledRecords);
        addMessage(message);
    }

    public void sendPartitionEvent(PartitionEventMessage.PartitionEventType type, Collection<TopicPartition> partitions) {
        var event = new PartitionEventMessage(type, partitions);
        log.debug("Adding {} to mailbox...", event);
        var message = ControllerEventMessage.<K, V>of(event);
        workMailBox.add(message);
    }

    // todo extract all to an event bus
    public void sendOffsetCommitSuccessEvent(CommitData event) {
        log.debug("Adding {} to mailbox...", event);
        var message = new ControllerEventMessage<K, V>(event);
        workMailBox.add(message);
    }

    public ControllerEventMessage<K, V> poll(long toMillis, TimeUnit milliseconds) throws InterruptedException {
        return workMailBox.poll(toMillis, milliseconds);
    }

    public int size() {
        return workMailBox.size();
    }

    public void drainTo(Queue<ControllerEventMessage<K, V>> results, int size) {
        workMailBox.drainTo(results, size);
    }

    public boolean isEmpty() {
        return workMailBox.isEmpty();
    }
}
