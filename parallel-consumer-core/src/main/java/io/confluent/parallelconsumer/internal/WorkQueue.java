package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkContainer;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * @author Antony Stubbs
 */
//todo remove? - only for new Worker queues branch experiment
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
