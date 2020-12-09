package io.confluent.parallelconsumer;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Cached view of the nested number of records in this queue
 */
public class CountingCRLinkedList<K, V> extends LinkedList<ConsumerRecords<K, V>> implements Queue<ConsumerRecords<K, V>> {

    @Getter
    private int nestedCount = 0;

    @Override
    public void add(final int index, final ConsumerRecords<K, V> element) {
        nestedCount = nestedCount + element.count();
        super.add(index, element);
    }

    @Override
    public ConsumerRecords<K, V> poll() {
        ConsumerRecords<K, V> poll = super.poll();
        nestedCount = nestedCount - poll.count();
        return poll;
    }
}
