package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.state.WorkContainer;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class WorkQueue<K, V> {

    Queue<List<WorkContainer<K, V>>> queue = new ArrayDeque<>();

    public int size() {
        return queue.size();
    }

    public List<WorkContainer<K, V>> poll() {
        return queue.poll();
    }

    public void add(List<WorkContainer<K, V>> work) {
        queue.add(work);
    }
}
