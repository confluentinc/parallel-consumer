package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.state.WorkContainer;

import java.util.ArrayDeque;
import java.util.Queue;

public class WorkQueue<K, V> {

    Queue<WorkContainer<K, V>> queue = new ArrayDeque<>();

    public int size() {
        return queue.size();
    }

    public WorkContainer<K, V> poll() {
        return queue.poll();
    }

    public void add(WorkContainer<K, V> work) {
        queue.add(work);
    }
}
