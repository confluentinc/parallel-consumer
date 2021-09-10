package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Cached view of the nested number of records in this queue.
 * <p>
 * Also protects against concurrent modification exceptions, as we don't need to traverse the list to count the nested
 * elements. The count won't then be always exact, but it doesn't need to be.
 */
@EqualsAndHashCode(callSuper = true)
public class CountingCRLinkedList<K, V> extends LinkedList<ConsumerRecords<K, V>> implements Queue<ConsumerRecords<K, V>> {

    /**
     * The number of nested {@link ConsumerRecord} in this collection. As this is a non blocking collection, this won't
     * be exact.
     */
    @Getter
    private int nestedCount = 0;

    @Override
    public void add(final int index, final ConsumerRecords<K, V> element) {
        nestedCount = nestedCount + element.count();
        super.add(index, element);
    }

    @Override
    public boolean add(final ConsumerRecords<K, V> element) {
        nestedCount = nestedCount + element.count();
        return super.add(element);
    }

    @Override
    public ConsumerRecords<K, V> poll() {
        ConsumerRecords<K, V> poll = super.poll();
        if (poll != null) {
            int numberOfNestedMessages = poll.count();
            nestedCount = nestedCount - numberOfNestedMessages;
        }
        return poll;
    }

}
